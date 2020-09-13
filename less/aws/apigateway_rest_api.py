def json_response(code, message, body=None):
    json_body = json.dumps(body) if body is not None else json.dumps({
        "message": message
    })
    return {
        "statusCode": code,
        "headers": cors_headers(),
        "body": json_body
    }


def cors_headers():
    return {
        "Access-Control-Allow-Origin": "*",
        "Access-Control-Allow-Headers": "*",
        "Access-Control-Allow-Methods": "GET, POST, DELETE, OPTIONS"
    }
