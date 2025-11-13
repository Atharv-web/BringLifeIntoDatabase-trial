from nt import execv
import asyncpg, asyncio
from config.settings import POSTGRES_URL, TIMESCALE_URL
from typing import List,Dict,Callable
import logging

logger = logging.getLogger(__name__)

class Database:
    """Database connection manager for postgres (source database) and Tigerdata (timescale database (meta database))
    Handles connection pooling, query execution,and listen/notify events.
    """

    def __init__(self):
        self.pg_pool = None #postgres pool connection
        self.ts_pool = None # timescaleDB pool connection
        self.listeners: Dict[str, asyncpg.Connection] = {}

    async def connect_db(self):
        """establish connection pools for both databases"""
        try:
            self.pg_pool = await asyncpg.create_pool(POSTGRES_URL,min_size=1,max_size=5)
            self.ts_pool = await asyncpg.create_pool(TIMESCALE_URL,min_size=1,max_size=5)
            logger.info("✅Connection pools established (POSTGRES AND TIGERDATA (TIMESCALE DB))")
            print("✅Connection pools established (POSTGRES AND TIGERDATA (TIMESCALE DB))")
        
        except Exception as e:
            logger.error(f"❌Database connection error occured: {e}")
            print(f"❌Database connection error occured: {e}")
            raise
    
    async def close_conn(self):
        """Close all connections and pools."""
        for channel,conn in self.listeners.items():
            try:
                await conn.close()
                logger.info(f"closed listener connection for channel: {channel}")
            except Exception as e:
                logger.error(f"error closing listener for {channel}: {e}")
        self.listeners.clear()

        if self.pg_pool:
            await self.pg_pool.close()
        if self.ts_pool:
            await self.ts_pool.close()
        
        logger.info("Database connections closed..")
        print("Database connections closed..")

#  =================== postgres database operations ======================

    async def fetch_pg_db(self, query:str, *args):
        """Run a select query and return rows"""
        async with self.pg_pool.acquire() as conn:
            return await conn.fetch(query, *args)
    
    async def fetchval_pg_db(self,query:str, *args):
        """fetch single value from postgres"""
        async with self.pg_pool.acquire() as conn:
            return await conn.fetchval(query,*args)
        
    async def fetchrow_pg_db(self,query:str,*args):
        """fetch single row from postgres"""
        async with self.pg_pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute_pg_db(self, query: str, *args):
        """Execute insert/update/delete on postgres db"""
        async with self.pg_pool.acquire() as conn:
            return await conn.execute(query, *args)

    # ===================== tigerdata operations ======================

    async def fetch_ts_db(self, query:str, *args):
        """Run a select query on timescaleDB"""
        async with self.ts_pool.acquire() as conn:
            return await conn.fetch(query, *args)

    async def fetchval_ts_db(self,query:str, *args):
        """fetch single value from postgres"""
        async with self.ts_pool.acquire() as conn:
            return await conn.fetchval(query,*args)
        
    async def fetchrow_ts_db(self,query:str,*args):
        """fetch single row from postgres"""
        async with self.ts_pool.acquire() as conn:
            return await conn.fetchrow(query, *args)

    async def execute_ts_db(self, query: str, *args):
        """Execute insert/update queries on timescale db"""
        async with self.ts_pool.acquire() as conn:
            return await conn.execute(query, *args)

    async def execute_ts_db(self,query:str, args_list: List[tuple]):
        """batch insert/update queries on timescale db"""
        async with self.ts_pool.acquire() as conn:
            return await conn.executemany(query, args_list)

# ================Listen/ NOTIFY events ========================

    async def listen_channel(self, channel: str, callback: Callable):
        """listen for postgres notify events on specific channels and keep channels alive.."""
        if channel in self.listeners:
            logger.warning(f"already listening on channel: {channel}")
            return

        try:        
            conn = await self.pg_pool.acquire()
            async def listener_callback(connection, pid, channel_name, payload):
                try:
                    await callback(payload)
                except Exception as e:
                    logger.error(f"Error in listener callback for {channel}: {e}")
            await conn.add_listener(channel,listener_callback)

            self.listeners[channel] =conn
            # await conn.add_listener(channel, lambda *args: asyncio.create_task(callback(args[-1])))
            logger.info(f"Listening on channel {channel}")
            print(f"Listening on channel {channel}")
        
        except Exception as e:
            logger.error("Failed to listen on {channel}: {e}")
            print(f"❌ Failed to listen on {channel}: {e}")
            raise
    
    async def unlisten_channel(self,channel:str):
        """stop listening on a channel and release the connections.."""
        if channel not in self.listeners:
            logger.warning(f"Not listening on channel: {channel}")
            return
            
        try:
            conn = self.listeners[channel]
            await conn.remove_listener(channel)
            await self.pg_pool.release(conn)
            del self.listeners[channel]
            logger.info(f"Stopped listening on: {channel}")
        except Exception as e:
            logger.error(f"Error unlistening from {channel}: {e}")

    async def notify(self,channel: str, payload:str):
        """send a notify event to postgres channel."""
        async with self.pg_pool.acquire() as conn:
            await conn.execute("SELECT pg_notify($1,$2)", channel,payload)
            logger.debug(f"Notified {channel}: {payload[:100]}")


# =================== utility methods ============================

    async def table_exists(self, table_name:str, schema: str="public", target:str="timescale") -> bool:
        """check if table exists in specified database."""

        query = """select exists (select 1 from information_schema.tables where table_schema =$1 AND table_name=$2);"""
        try:
            if target == "timescale":
                return await self.fetchval_ts_db(query,schema,table_name)
            else:
                return await self.fetchval_pg_db(query,schema,table_name)
        except Exception as e:
            logger.error(f"Error checking table existence: {e}")
            return False

    async def table_has_data(self, table_name:str,schema:str="public" ,target:str="timescale") -> bool:
        """check if tables contain any rows/data.."""

        query = f"""select count(*) from {schema}.{table_name};"""
        try:
            if target =="timescale":
                count = await self.fetchval_ts_db(query)
            else:
                count = await self.fetchval_pg_db(query)
            return count >0
        except Exception as e:
            logger.error(f"error checking table data: {e}")
            return False

    async def get_table_row_count(self,table_name:str,schema:str= "public",target:str = "timescale") -> int:
        """get the row count for a table"""
        
        query = f"""select count (*) from {schema}.{table_name}"""
        try: 
            if target == "timescale":
                return await self.fetchval_ts_db(query)
            else:
                return await self.fetchval_pg_db(query)
        except Exception as e:
            logger.error(f"Error getting row count: {e}")
            return 0

    async def test_connection(self) -> bool:
        """test if both database connections are working.."""
        try:
            pg_db_test_conn = await self.fetchval_pg_db("""SELECT 1""")
            ts_db_test_conn = await self.fetchval_ts_db("""SELECT 1""")
            return pg_db_test_conn ==1 and ts_db_test_conn ==1
        except Exception as e:
            logger.error(f"Connection test failed : {e}")
            return False