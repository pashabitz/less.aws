import json


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


class AccessTokenAuthorizer(object):
    def __init__(self, jwks_url, audience, issuer, algorithms=["RS256"]):
        self.jwks_url = jwks_url
        self.audience = audience
        self.issuer = issuer
        self.algorithms = algorithms

    @staticmethod
    def _get_access_token(headers):
        if "Authorization" not in headers:
            return None
        header = headers["Authorization"]
        parts = header.split()
        if len(parts) != 2 or parts[0].lower() != "bearer":
            return None
        return parts[1]

    def verify_access_token(self, headers):
        token = AccessTokenAuthorizer._get_access_token(headers)
        if token is None:
            raise InvalidRequestException("Missing access token")
        jwks = requests.get(self.jwks_url).json()
        unverified_header = jwt.get_unverified_header(token)
        access_token_key_id = unverified_header["kid"]
        rsa_key = {}
        for key in jwks["keys"]:
            if key["kid"] == access_token_key_id:
                rsa_key = {
                    "kty": key["kty"],
                    "kid": key["kid"],
                    "use": key["use"],
                    "n": key["n"],
                    "e": key["e"]
                }
        if not rsa_key:
            raise InvalidRequestException(f"Key with id {access_token_key_id} not found")
        try:
            payload = jwt.decode(
                token,
                rsa_key,
                algorithms=self.algorithms,
                audience=self.audience,
                issuer=self.issuer,
            )
            return payload
        except jwt.ExpiredSignatureError:
            raise InvalidRequestException("Access token expired")
        except jwt.JWTClaimsError:
            raise InvalidRequestException("Invalid claims in access token; check the audience and issuer")
        except Exception:
            raise InvalidRequestException("Unable to validate access token")
