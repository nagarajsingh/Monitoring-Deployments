import asyncio
import base64
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from fastapi import FastAPI, Query, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', extra='ignore')
    azdo_organization: str
    azdo_project: str
    azdo_pat: str
    poll_interval_seconds: int = 15
    completed_build_limit: int = 200
    verify_ssl: bool = True


settings = Settings()
INDEX = Path(__file__).parent / 'index.html'
GTB_KEYWORDS = ('obdx', 'obtfpm', 'oblm', 'oblmic', 'obvam', 'obvamic', 'plato', 'cmncore', 'moc', 'obp', 'obtf')
cache: dict[str, Any] = {
    'mode': 'live', 'selectedDate': None, 'updatedAt': None, 'summary': {},
    'runningBuilds': [], 'queuedBuilds': [], 'completedBuilds': [],
    'waitingDeployments': [], 'errors': []
}
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


def category_for(name: str | None) -> str:
    value = (name or '').lower()
    if 'collection' in value:
        return 'collections'
    if any(keyword in value for keyword in GTB_KEYWORDS):
        return 'gtb'
    return 'microservices'


def build_view(item: dict[str, Any]) -> dict[str, Any]:
    pipeline = (item.get('definition') or {}).get('name', 'Unknown')
    return {
        'id': item.get('id'),
        'number': item.get('buildNumber'),
        'pipeline': pipeline,
        'category': category_for(pipeline),
        'branch': (item.get('sourceBranch') or '').removeprefix('refs/heads/'),
        'status': item.get('status'),
        'result': item.get('result'),
        'requestedBy': (item.get('requestedFor') or {}).get('displayName'),
        'startTime': item.get('startTime'),
        'finishTime': item.get('finishTime'),
        'durationSeconds': duration(item.get('startTime'), item.get('finishTime')),
        'url': item.get('_links', {}).get('web', {}).get('href'),
    }


async def load_builds(
    client: httpx.AsyncClient,
    status: str,
    top: int = 100,
    min_time: str | None = None,
    max_time: str | None = None,
) -> list[dict[str, Any]]:
    base = f'https://dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    params: dict[str, Any] = {
        'statusFilter': status,
        '$top': top,
        'queryOrder': 'finishTimeDescending' if status == 'completed' else 'queueTimeDescending',
        'api-version': '7.1',
    }
    if min_time:
        params['minTime'] = min_time
    if max_time:
        params['maxTime'] = max_time
    payload = await get_json(client, f'{base}/_apis/build/builds', params)
    return payload.get('value', [])


async def load_approvals(client: httpx.AsyncClient) -> tuple[list[dict[str, Any]], list[str]]:
    approvals: list[dict[str, Any]] = []
    errors: list[str] = []
    base = f'https://dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    release_base = f'https://vsrm.dev.azure.com/{settings.azdo_organization}/{settings.azdo_project}'
    try:
        payload = await get_json(client, f'{base}/_apis/pipelines/approvals', {
            'state': 'pending', '$top': 100, 'api-version': '7.1-preview.1'
        })
        for item in payload.get('value', []):
            resource = item.get('resource') or {}
            pipeline = (item.get('pipeline') or {}).get('name') or resource.get('name') or 'Pipeline deployment'
            approvals.append({
                'id': item.get('id'), 'type': 'YAML', 'pipeline': pipeline,
                'category': category_for(pipeline),
                'environment': resource.get('name') or item.get('stageName') or 'Environment',
                'status': item.get('status') or item.get('state') or 'pending',
                'createdOn': item.get('createdOn') or item.get('createdDate'),
                'requestedBy': (item.get('createdBy') or {}).get('displayName'),
                'url': item.get('_links', {}).get('web', {}).get('href'),
            })
    except Exception:
        errors.append('YAML approvals are unavailable. Verify PAT permissions and Azure DevOps API access.')
    try:
        payload = await get_json(client, f'{release_base}/_apis/release/approvals', {
            'statusFilter': 'pending', '$top': 100, 'api-version': '7.1'
        })
        for item in payload.get('value', []):
            release = item.get('release') or {}
            environment = item.get('releaseEnvironment') or {}
            pipeline = release.get('name') or 'Classic release'
            approvals.append({
                'id': item.get('id'), 'type': 'Classic', 'pipeline': pipeline,
                'category': category_for(pipeline),
                'environment': environment.get('name') or 'Environment',
                'status': item.get('status', 'pending'), 'createdOn': item.get('createdOn'),
                'requestedBy': (item.get('approver') or {}).get('displayName'),
                'url': release.get('_links', {}).get('web', {}).get('href'),
            })
    except Exception:
        pass
    return approvals, errors


def response_payload(
    running: list[dict[str, Any]], queued: list[dict[str, Any]],
    completed: list[dict[str, Any]], approvals: list[dict[str, Any]],
    errors: list[str], mode: str = 'live', selected_date: str | None = None,
) -> dict[str, Any]:
    running_views = [build_view(x) for x in running]
    queued_views = [build_view(x) for x in queued]
    completed_views = [build_view(x) for x in completed]
    return {
        'mode': mode, 'selectedDate': selected_date,
        'updatedAt': datetime.now(timezone.utc).isoformat(),
        'summary': {
            'running': len(running_views), 'queued': len(queued_views),
            'completed': len(completed_views),
            'succeeded': sum(x.get('result') == 'succeeded' for x in completed_views),
            'failed': sum(x.get('result') == 'failed' for x in completed_views),
            'cancelled': sum(x.get('result') == 'canceled' for x in completed_views),
            'waitingDeployment': len(approvals),
        },
        'runningBuilds': running_views, 'queuedBuilds': queued_views,
        'completedBuilds': completed_views, 'waitingDeployments': approvals,
        'errors': errors,
    }


async def collect_live() -> dict[str, Any]:
    errors: list[str] = []
    async with httpx.AsyncClient(timeout=20, verify=settings.verify_ssl) as client:
        results = await asyncio.gather(
            load_builds(client, 'inProgress'),
            load_builds(client, 'notStarted'),
            load_builds(client, 'completed', settings.completed_build_limit),
            return_exceptions=True,
        )
        values: list[list[dict[str, Any]]] = []
        for label, result in zip(('running builds', 'queued builds', 'completed builds'), results):
            if isinstance(result, Exception):
                errors.append(f'Unable to load {label}: {result}')
                values.append([])
            else:
                values.append(result)
        approvals, approval_errors = await load_approvals(client)
        errors.extend(approval_errors)
    return response_payload(values[0], values[1], values[2], approvals, errors)


async def collect_history(selected_date: str, min_time: str, max_time: str) -> dict[str, Any]:
    errors: list[str] = []
    try:
        async with httpx.AsyncClient(timeout=30, verify=settings.verify_ssl) as client:
            completed = await load_builds(
                client, 'completed', max(settings.completed_build_limit, 1000), min_time, max_time
            )
    except Exception as exc:
        completed = []
        errors.append(f'Unable to load builds for {selected_date}: {exc}')
    return response_payload([], [], completed, [], errors, 'history', selected_date)


async def poll() -> None:
    global cache
    while True:
        try:
            cache = await collect_live()
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
async def dashboard(
    date: str | None = Query(default=None, pattern=r'^\d{4}-\d{2}-\d{2}$'),
    min_time: str | None = None,
    max_time: str | None = None,
) -> dict[str, Any]:
    if not date:
        return cache
    if not min_time or not max_time:
        return {
            'mode': 'history', 'selectedDate': date, 'updatedAt': datetime.now(timezone.utc).isoformat(),
            'summary': {}, 'runningBuilds': [], 'queuedBuilds': [], 'completedBuilds': [],
            'waitingDeployments': [], 'errors': ['Date boundaries are required.']
        }
    return await collect_history(date, min_time, max_time)


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
