import json
import os
import sys
import urllib.error
import urllib.request

SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://127.0.0.1:8081")

SCHEMAS_TO_REGISTER = [
    {
        "subject": "raw_events-value",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "contracts", "schemas", "normalized_webhook_event.json"),
        "schema_type": "JSON"
    },
    {
        "subject": "reply_commands-value",
        "file_path": os.path.join(os.path.dirname(__file__), "..", "contracts", "schemas", "reply_command.json"),
        "schema_type": "JSON"
    }
]

def make_request(url, method="GET", data=None):
    headers = {"Content-Type": "application/vnd.schemaregistry.v1+json"}
    body = json.dumps(data).encode("utf-8") if data is not None else None
    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            resp_body = resp.read().decode("utf-8")
            return resp.status, json.loads(resp_body) if resp_body else {}
    except urllib.error.HTTPError as e:
        error_body = e.read().decode("utf-8")
        try:
            parsed = json.loads(error_body)
            return e.code, parsed
        except Exception:
            return e.code, {"error": error_body}
    except Exception as e:
        print(f"❌ Connection error to {url}: {e}")
        return None, None

def set_compatibility(subject, compatibility="BACKWARD"):
    url = f"{SCHEMA_REGISTRY_URL.rstrip('/')}/config/{subject}"
    status, res = make_request(url, method="PUT", data={"compatibility": compatibility})
    if status == 200:
        print(f"✅ Set compatibility '{compatibility}' for subject '{subject}'")
    else:
        print(f"⚠️ Warning: Could not set compatibility for {subject}: {res}")

def register_schema(subject, file_path, schema_type="JSON"):
    if not os.path.exists(file_path):
        print(f"❌ Schema file not found: {file_path}")
        return False

    with open(file_path, "r", encoding="utf-8") as f:
        schema_content = f.read()

    # Minify or clean JSON schema string for Schema Registry payload
    parsed_schema = json.loads(schema_content)
    clean_schema_str = json.dumps(parsed_schema)

    payload = {
        "schemaType": schema_type,
        "schema": clean_schema_str
    }

    url = f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects/{subject}/versions"
    status, res = make_request(url, method="POST", data=payload)
    if status == 200 and "id" in res:
        print(f"✅ Successfully registered schema for subject '{subject}' (Schema ID: {res['id']})")
        return True
    else:
        print(f"❌ Failed to register schema for subject '{subject}': {res}")
        return False

def check_registered_subjects():
    url = f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects"
    status, res = make_request(url, method="GET")
    if status == 200:
        print(f"\n📋 Currently registered subjects in Schema Registry ({SCHEMA_REGISTRY_URL}):")
        for sub in res:
            print(f"   - {sub}")
    else:
        print(f"❌ Could not retrieve subjects: {res}")

if __name__ == "__main__":
    print(f"🔌 Checking Confluent Schema Registry at: {SCHEMA_REGISTRY_URL}")
    status, _ = make_request(f"{SCHEMA_REGISTRY_URL.rstrip('/')}/subjects")
    if status is None:
        print("❌ Cannot connect to Schema Registry. Make sure Docker container 'schema-registry' is running.")
        sys.exit(1)

    print("🚀 Registering Data Contracts (Schemas)...")
    for item in SCHEMAS_TO_REGISTER:
        set_compatibility(item["subject"], "BACKWARD")
        register_schema(item["subject"], item["file_path"], item["schema_type"])

    check_registered_subjects()
    print("\n🎉 Data Contracts registration complete!")

