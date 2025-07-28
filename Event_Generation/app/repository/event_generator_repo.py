import random
from faker import Faker
from faker.providers import DynamicProvider
from app.constant.event_constants import EVENT_TYPES, EVENT_SENDERS, EVENT_TENANTS

fake = Faker()

entity_types = list(EVENT_TYPES.keys())

# Providers for event_name and entity_type
entity_type_provider = DynamicProvider(
    provider_name="entity_type",
    elements=entity_types,
)

# Flatten all event names for provider
all_event_names = [name for names in EVENT_TYPES.values() for name in names]

event_name_provider = DynamicProvider(
    provider_name="event_name",
    elements=all_event_names,
)

fake.add_provider(entity_type_provider)
fake.add_provider(event_name_provider)

def generate_policy_details():
    return {
        "policy_id": fake.uuid4(),
        "policy_number": fake.bothify(text="POL#######"),
        "policy_type": fake.random_element(elements=["auto", "home", "life", "health"]),
        "policy_status": fake.random_element(elements=["active", "expired", "pending", "suspended"]),
        "start_date": fake.date_between(start_date="-3y", end_date="-1y"),
        "end_date": fake.date_between(start_date="-1y", end_date="+2y"),
        "premium_amount": fake.pydecimal(left_digits=5, right_digits=2, positive=True),
        "currency": fake.currency_code(),
        "beneficiaries": [
            {
                "name": fake.name(),
                "relation": fake.random_element(elements=["spouse", "child", "parent", "sibling"]),
                "percentage": random.randint(10, 100)
            } for _ in range(random.randint(1, 3))
        ],
        "agent": {
            "agent_id": fake.uuid4(),
            "agent_name": fake.name(),
            "agent_email": fake.email(),
        },
    }

def generate_claim_details():
    return {
        "claim_id": fake.uuid4(),
        "claim_number": fake.bothify(text="CLM#######"),
        "claim_type": fake.random_element(elements=["accident", "theft", "fire", "medical"]),
        "claim_status": fake.random_element(elements=["open", "closed", "in_review", "approved", "rejected"]),
        "claim_amount": fake.pydecimal(left_digits=4, right_digits=2, positive=True),
        "currency": fake.currency_code(),
        "incident_date": fake.date_between(start_date="-2y", end_date="now"),
        "reported_date": fake.date_between(start_date="-2y", end_date="now"),
        "adjuster": {
            "adjuster_id": fake.uuid4(),
            "adjuster_name": fake.name(),
            "adjuster_email": fake.email(),
        },
        "documents": [
            {
                "doc_id": fake.uuid4(),
                "doc_type": fake.random_element(elements=["photo", "report", "invoice"]),
                "uploaded_at": fake.date_time_this_year()
            } for _ in range(random.randint(1, 4))
        ],
    }

def generate_document_details():
    return {
        "document_id": fake.uuid4(),
        "document_type": fake.random_element(elements=["pdf", "image", "text", "spreadsheet"]),
        "uploaded_by": fake.name(),
        "uploaded_at": fake.date_time_this_year(),
        "status": fake.random_element(elements=["uploaded", "verified", "shared", "deleted"]),
    }

def generate_account_details():
    return {
        "account_id": fake.uuid4(),
        "account_type": fake.random_element(elements=["savings", "checking", "credit", "loan"]),
        "account_status": fake.random_element(elements=["active", "suspended", "closed", "reactivated"]),
        "opened_date": fake.date_between(start_date="-5y", end_date="now"),
        "balance": fake.pydecimal(left_digits=6, right_digits=2, positive=True),
        "currency": fake.currency_code(),
    }

def generate_user_details():
    return {
        "user_id": fake.uuid4(),
        "username": fake.user_name(),
        "email": fake.email(),
        "registered_at": fake.date_time_between(start_date="-3y", end_date="now"),
        "status": fake.random_element(elements=["active", "locked", "deleted"]),
        "role": fake.job(),
    }

# 25 aggregate_id combinations from governance table
AGGREGATE_IDS = [
    "QA#API#AONE.Cancelled",
    "QA#PORTAL#DFOUR.Deleted",
    "TDB#SYSTEM#EFIVE.Updated",
    "TST#USER#BTWO.Updated",
    "TST#API#DFOUR.Shared",
    "DEV#PORTAL#AONE.Initiated",
    "TDB#SYSTEM#DFOUR.Verified",
    "TDB#USER#EFIVE.Created",
    "TST#MS#EFIVE.Deleted",
    "PRD#SYSTEM#DFOUR.Uploaded",
    "TST#API#BTWO.Locked",
    "TDB#MS#CTHREE.Closed",
    "TST#USER#EFIVE.Reactivated",
    "QA#USER#AONE.Completed",
    "QA#API#CTHREE.Approved",
    "DEV#PORTAL#CTHREE.Submitted",
    "PRD#MS#DFOUR.Updated",
    "DEV#USER#CTHREE.Rejected",
    "TST#MS#AONE.Updated",
    "DEV#PORTAL#BTWO.Deleted",
    "TDB#SYSTEM#EFIVE.Suspended",
    "DEV#SYSTEM#CTHREE.Failed",
    "TDB#SYSTEM#AONE.Renewed",
    "PRD#API#BTWO.Registered",
    "DEV#SYSTEM#BTWO.Initiated"
]

# Helper to get event_type from event_name
EVENT_NAME_TO_TYPE = {ename: etype for etype, enames in EVENT_TYPES.items() for ename in enames}

def generate_event_data():
    event_data = []
    # 25 aggregate_id events + 5 random
    pool = AGGREGATE_IDS.copy()
    
    for _ in range(5):
        tenant = random.choice(EVENT_TENANTS)
        sender = random.choice(EVENT_SENDERS)
        event_name = random.choice(all_event_names)
        pool.append(f"{tenant}#{sender}#{event_name}")
    
    for _ in range(10):
        agg_id = random.choice(pool)
        tenant, sender, event_name = agg_id.split("#")
        event_type = EVENT_NAME_TO_TYPE.get(event_name, random.choice(entity_types))
        event_id = fake.uuid4()
        event_timestamp = fake.date_time_between(start_date="-1y", end_date="now")
        event_user_id = fake.uuid4()
        event_user_name = fake.name()
        event_user_email = fake.email()
        event_user_phone = fake.phone_number()
        event_user_address = fake.address()
        event_user_city = fake.city()
        event_user_state = fake.state()
        event_user_zip = fake.zipcode()
        event_user_country = fake.country()
        event_user_ip_address = fake.ipv4()
        event_user_browser = fake.user_agent()
        event_user_device = fake.random_element(elements=["mobile", "desktop", "tablet", "iot"])
        event_user_os = fake.random_element(elements=["Windows", "macOS", "Linux", "Android", "iOS"])
        event_user_language = fake.language_code()
        event_user_timezone = fake.timezone()
        event_user_company = fake.company()
        event_user_department = fake.random_element(elements=["sales", "support", "claims", "underwriting", "it"])
        event_user_role = fake.job()
        event_user_last_login = fake.date_time_between(start_date="-2y", end_date="now")
        event_user_status = fake.random_element(elements=["active", "inactive", "locked", "pending"])
        event_metadata = {
            "source": sender,
            "tenant": tenant,
            "correlation_id": fake.uuid4(),
            "request_id": fake.uuid4(),
            "received_at": fake.date_time_between(start_date="-1y", end_date="now"),
            "processed_at": fake.date_time_between(start_date="-1y", end_date="now"),
            "priority": fake.random_element(elements=["low", "medium", "high"]),
            "tags": fake.words(nb=random.randint(2, 5)),
            "version": fake.random_element(elements=["v1", "v2", "v3"]),
        }
        # Nested event details based on entity type
        if event_type == "Policy":
            event_details = generate_policy_details()
        elif event_type == "Claim":
            event_details = generate_claim_details()
        elif event_type == "Document":
            event_details = generate_document_details()
        elif event_type == "Account":
            event_details = generate_account_details()
        elif event_type == "User":
            event_details = generate_user_details()
        else:
            event_details = {}

        event_data.append({
            "event_id": event_id,
            "event_type": event_type,
            "event_name": event_name,
            "event_sender": sender,
            "event_tenant": tenant,
            "event_timestamp": event_timestamp.isoformat(),
            "event_user": {
                "user_id": event_user_id,
                "name": event_user_name,
                "email": event_user_email,
                "phone": event_user_phone,
                "address": {
                    "street": event_user_address,
                    "city": event_user_city,
                    "state": event_user_state,
                    "zip": event_user_zip,
                    "country": event_user_country,
                },
                "ip_address": event_user_ip_address,
                "browser": event_user_browser,
                "device": event_user_device,
                "os": event_user_os,
                "language": event_user_language,
                "timezone": event_user_timezone,
                "company": event_user_company,
                "department": event_user_department,
                "role": event_user_role,
                "last_login": event_user_last_login.isoformat(),
                "status": event_user_status,
            },
            "event_device_info": {
                "ip_address": event_user_ip_address,
                "browser": event_user_browser,
                "device": event_user_device,
                "os": event_user_os,
                "location": {
                    "city": event_user_city,
                    "country": event_user_country,
                    "timezone": event_user_timezone,
                },
            },
            "event_details": event_details,
            "event_metadata": event_metadata,
            # Add more top-level attributes for complexity
            "is_test_event": fake.boolean(),
            "environment": fake.random_element(elements=["dev", "qa", "prod"]),
            "application": fake.random_element(elements=["portal", "mobile_app", "api_gateway"]),
            "region": fake.random_element(elements=["us-east-1", "eu-west-1", "ap-south-1"]),
            "retry_count": random.randint(0, 5),
            "error_info": {
                "error_code": fake.random_element(elements=[None, "E100", "E200", "E404", "E500"]),
                "error_message": fake.random_element(elements=[None, "Timeout", "Not Found", "Internal Error", "Validation Failed"]),
            },
            "custom_fields": {
                fake.word(): fake.word() for _ in range(5)
            },
        })
    return event_data 