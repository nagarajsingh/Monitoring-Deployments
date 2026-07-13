import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    azdo_organization: str
    azdo_project: str
    azdo_pat: str
    poll_interval_seconds: int = 15
    completed_build_limit: int = 50
    verify_ssl: bool = True


settings = Settings()
INDEX = Path(__file__).parent / 'index.html'
cache: dict[str, Any] = {'updatedAt': None, 'summary': {}, 'runningBuilds': [], 'completedBuilds': [], 'waitingDeployments': [], 'errors': []}
clients: set[WebSocket] = set()


def auth_headers() -> dict[str, str]:
    token = base64.b64encode(f':{settings.azdo_pat}'.encode()).decode()
    return {'Authorization': f'Basic {token}', 'Accept': 'application/json'}


async def get_json(client: httpx.AsyncClient, url: str, params: dict[str, Any]) -> dict[str, Any]:
    response = await client.get(url, headers=auth_headers(), params=params)
    response.raise_for_status()
    return response.json()


def duration(start: str | None, finish: str | None = None) -> int | None:
    if not start:
        return None
    start_dt = datetime.fromisoformat(start.replace('Z', '+00:00'))
    end_dt = datetime.fromisoformat(finish.replace('Z', '+00:00')) if finish else datetime.now(timezone.utc)
    return max(0, int((end_dt - start_dt).total_seconds()))


def build_view(item: dict[str, Any]) -> dict[str, Any]:
    return {
        'id': item.get('id'),
        'number': item.get('buildNumber'),
        'pipeline': (item.get('definition') or {}).get('name', 'Unknown'),
        'branch': (item.get('sourceBranch') or '').removeprefix('refs/heads/'),
        'status': item.get('status'),
        'result': item.get('result'),
        'requestedBy': (item.get('requestedFor') or {}).get('displayName'),
        'startTime': item.get('startTime'),
        'finishTime': item.get('finishTime'),
        'durationSeconds': duration(item.get('startTime'), item.get('finishTime')),
        'url': item.get('_links', {}).get('web', {}).get('href'),
    }


async def load_builds(client: httpx.AsyncClient, status: str, top: int = 100) -> list[dict[str, Any]]:
    base = f'https://dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    payload = await get_json(client, f'{base}/_apis/build/builds', {
        'statusFilter': status,
        '$top': top,
        'queryOrder': 'queueTimeDescending',
        'api-version': '7.1',
    })
    return payload.get('value', [])


async def load_approvals(client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], list[str]]:
    approvals: list[dict[str, Any]] = []
    errors: list[str] = []
    base = f'https://dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    release_base = f'https://vsrm.dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    try:
        payload = await get_json(client, f'{base}/_apis/pipelines/approvals', {'state': 'pending', '$top': 100, 'api-version': '7.1-preview.1'})
        for item in payload.get('value', []):
            resource = item.get('resource') or {}
            approvals.append({
                'id': item.get('id'), 'type': 'YAML',
                'pipeline': (item.get('pipeline') or {}).get('name') or resource.get('name') or 'Pipeline deployment',
                'environment': resource.get('name') or item.get('stageName') or 'Environment',
                'status': item.get('status') or item.get('state') or 'pending',
                'createdOn': item.get('createdOn') or item.get('createdDate'),
                'requestedBy': (item.get('createdBy') or {}).get('displayName'),
                'url': item.get('_links', {}).get('web', {}).get('href'),
            })
    except Exception:
        errors.append('YAML approvals are unavailable. Verify PAT permissions and Azure DevOps API access.')
    try:
        payload = await get_json(client, f'{release_base}/_apis/release/approvals', {'statusFilter': 'pending', '$top': 100, 'api-version': '7.1'})
        for item in payload.get('value', []):
            release = item.get('release') or {}
            environment = item.get('releaseEnvironment') or {}
            approvals.append({
                'id': item.get('id'), 'type': 'Classic',
                'pipeline': release.get('name') or 'Classic release',
                'environment': environment.get('name') or 'Environment',
                'status': item.get('status', 'pending'),
                'createdOn': item.get('createdOn'),
                'requestedBy': (item.get('approver') or {}).get('displayName'),
                'url': release.get('_links', {}).get('web', {}).get('href'),
            })
    except Exception:
        pass
    return approvals, errors


async def collect() -> dict[str, Any]:
    errors: list[str] = []
    async with httpx.AsyncClient(timeout=20, verify=settings.verify_ssl) as client:
        results = await asyncio.gather(
            load_builds(client, 'inProgress'),
            load_builds(client, 'notStarted'),
            load_builds(client, 'completed', settings.completed_build_limit),
            return_exceptions=True,
        )
        values = []
        for label, result in zip(('running builds', 'queued builds', 'completed builds'), results):
            if isinstance(result, Exception):
                errors.append(f'Unable to load {label}: {result}')
                values.append([])
            else:
                values.append(result)
        running, queued, completed = values
        approvals, approval_errors = await load_approvals(client)
        errors.extend(approval_errors)

    completed_views = [build_view(x) for x in completed]
    return {
        'updatedAt': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'running': len(running), 'queued': len(queued), 'completed': len(completed_views),
            'succeeded': sum(x.get('result') == 'succeeded' for x in completed_views),
            'failed': sum(x.get('result') == 'failed' for x in completed_views),
            'waitingDeployment': len(approvals),
        },
        'runningBuilds': [build_view(x) for x in running],
        'queuedBuilds': [build_view(x) for x in queued],
        'completedBuilds': completed_views,
        'waitingDeployments': approvals,
        'errors': errors,
    }


async def poll() -> None:
    global cache
    while True:
        try:
            cache = await collect()
            dead = []
            for socket in clients:
                try:
                    await socket.send_json(cache)
                except Exception:
                    dead.append(socket)
            for socket in dead:
                clients.discard(socket)
        except Exception as exc:
            cache['errors'] = [f'Refresh failed: {exc}']
        await asyncio.sleep(max(5, settings.poll_interval_seconds))


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(poll())
    yield
    task.cancel()


app = FastAPI(title='Azure DevOps Delivery Dashboard', lifespan=lifespan)


@app.get('/')
async def index() -> FileResponse:
    return FileResponse(INDEX)


@app.get('/api/dashboard')
async def dashboard() -> dict[str, Any]:
    return cache


@app.get('/healthz')
async def health() -> dict[str, str]:
    return {'status': 'ok'}


@app.get('/readyz')
async def ready() -> dict[str, str]:
    return {'status': 'ready'}


@app.websocket('/ws')
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    clients.add(websocket)
    await websocket.send_json(cache)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.discard(websocket)
