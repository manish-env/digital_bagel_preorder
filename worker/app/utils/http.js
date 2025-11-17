export function json(obj, status = 200, extraHeaders = {}) {
    const headers = { 'content-type': 'application/json', ...extraHeaders };
    return new Response(JSON.stringify(obj), {
        status,
        headers
    });
}


