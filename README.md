# Api Sync Client
a synchronous python client to interact with the data exchange api.
This script aims to showcase the sequence flow of synchronising data stores against the focus api solution.

### Sequence Flow

| Step | Description                                                 | Example |
|------|-------------------------------------------------------------|---------|
| 1    | Authenticate                                                | `/api/v1/auth` |
| 2    | Load the resource schema                                    | `/api/v1/{BUSINESS_SERVICE}/resource/schema` |
| 3    | Build DB schema if it doesn't exist (idempotent)           | |
| 4    | Get client-side resource state (e.g. latest timestamp)      | |
| 5    | Decide between full or delta load                           | If table empty → full; else → delta |
|      |*↓ depending on decision:*               |                                  *6.a &nbsp;&nbsp;&nbsp;&nbsp; <strong>VS.</strong> &nbsp;&nbsp;&nbsp; 6.b ➡️ 6.c ➡️ 6.d*                                        |
| 6a   | Load full resource                                           | `/api/v1/{BUSINESS_SERVICE}/resource/{RESOURCE_NAME}/{COUNTRY}/full/lines` |
| 6b   | Load newly created items                                     | `/api/v1/{BUSINESS_SERVICE}/resource/{RESOURCE_NAME}/{COUNTRY}/created/lines?delta_timestamp=2025-07-01T16:01:00.827904` |
| 6c   | Load updated items                                           | `/api/v1/{BUSINESS_SERVICE}/resource/{RESOURCE_NAME}/{COUNTRY}/updated/lines?delta_timestamp=2025-07-01T16:01:00.827904` |
| 6d   | Load deleted item IDs                                        | `/api/v1/{BUSINESS_SERVICE}/resource/{RESOURCE_NAME}/{COUNTRY}/deleted/lines?delta_timestamp=2025-07-01T16:01:00.827904` |
| 7    | Check server state                                           | `/api/v1/{BUSINESS_SERVICE}/resource/{RESOURCE_NAME}/{COUNTRY}/state?delta_timestamp=2025-07-01T16:01:00.827904` |
| 8    | Compare client and server state                              | Compare `COUNT(*)` from client table with `payload.total_items` |


### How to use
the app is written in python:3.12.
Here is what you need to do:
|  |  |
| ------- | ------  |
| Clone Repository | ```git clone https://github.com/focus-international-services/api_sync_client.git```|
| Install Dependencies | ```pip install -r requirements.txt``` |
| Adjust Configuration | change settings to your credentials in config.yml |
| start Client Database | ```docker compose up -d``` |
| run system | ```python main.py``` |
