# insert
async def run(server):
    from fastapi import Request
    from typing import Union
    log = server.log

    @server.api_route('/db/{database}/table/{table}/insert', methods=['POST'])
    async def insert_func_api(database: str, table: str, params: Union[dict, list], request: Request):
        return await insert_func(database, table, params,  request=await server.process_request(request))

    @server.is_authenticated('local')
    async def insert_func(database, table, params, **kw):
        return await insert(database, table, params, **kw)

    def parse_params(params):
        if isinstance(params, list):
            for item in params:
                if not isinstance(item, dict):
                    error = f"insert error - invalid type {type(item)} provided inside list params item - {item}"
                    server.http_exception(400, log.error(error))

    async def insert(database, table, params, **kw):
        message, rc = await server.check_db_table_exist(database,table)
        if rc == 200:
            table = server.data[database].tables[table]

            async def insert_to_db(values):
                try:
                    response = await table.insert(**values)
                except Exception as e:
                    server.http_exception(
                        400, 
                        log.exception(f"error inserting into {database} {table} using {params} - {repr(e)}")
                        )

            # Multi Insert
            if isinstance(params, list):
                multi_insert = []
                for item in params:
                    if not isinstance(item, dict):
                        error = f"insert error - invalid type {type(item)} provided inside list params item - {item}"
                        server.http_exception(400, log.error(error))

                    for k,v in item.items(): 
                        if not k in table.columns:
                            error = f"invalid key provided '{k}' not found in table {table.name}, valid keys {[col for col in table.columns]}"
                            log.error(error)
                            server.http_exception(400, error)
                    multi_insert.append(
                        insert_to_db(item)
                    )
                await asyncio.gather(*multi_insert)

            # Single Insertion
            else:
                for k,v in params.items(): 
                    if not k in table.columns:
                        error = f"invalid key provided '{k}' not found in table {table.name}, valid keys {[col for col in table.columns]}"
                        log.error(error)
                        server.http_exception(400, error)
                try:
                    response = await table.insert(**params)
                except Exception as e:
                    server.http_exception(
                        400, 
                        log.exception(f"error inserting into {database} {table} using {params} - {repr(e)}")
                        )
            return {"message": "items added"}
        else:
            server.http_exception(rc, message)
    server.actions['insert'] = insert