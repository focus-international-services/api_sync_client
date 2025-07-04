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


### Install
the app is written in python:3.12.
Here is what you need to do:

`git clone`






# step 1:
        api_reader.authenticate()

# step2: load the resource schema
        schema = api_reader.fetch_schema()

# step3: build up the database schema if it does not exist (idempotent)
        api_reader.db.schema = schema
        api_reader.db.create_tables(schema)

# step4: generally this could be done for multiple resources in parallel but for
              the demonstration it only regards a single resource promotions
              extract the latest state from the client side for the resource promotions
        promotions = api_reader.db.schema.resources[0]
        change_ts = api_reader.db.latest_state(promotions.name)
        logger.info(f"client state timestamp: {change_ts}")

# step 5: based on the change_ts decide whether a full load or a delta load is required and perform it.
                generally it does not matter in which sequence the delta load is performed, as no item is in multiple
                 endpoints

# step 6: Check the client state vs the server state to see if both are in sync.


