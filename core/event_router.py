from ast import Call
import asyncio,json
from typing import Callable,Dict,Any,List, final
from core.database import Database
import logging

logger = logging.getLogger(__name__)


class EventRouter:
    """Event based communication layuer between database and agents. 
    it listens to database notify events and triggers relevant agent callbacks.
    """

    def __init__(self,db:Database) -> None:
        self.db=db
        self.subscribers: Dict[str, Callable[[dict],Any]] = {}
        self.running = False
        self.listener_task = None

    def subscribe(self, channel:str, callback: Callable[[dict],Any]):
        """register a callback for a specific event channel.
        agents use this to subscribe to event notifications.
        """
        if channel not in self.subscribers:
            self.subscribers[channel] = []

        self.subscribers[channel].append(callback)
        logger.info(f"agent subscribed to channel: {channel}")
        print(f"agent subscribed to channel: {channel}")

    def unsubscribe(self,channel:str, callback: Callable[[dict],Any]):
        """remove a specific callback from a channel."""
        if channel in self.subscribers:
            try:
                self.subscribers[channel].remove(callback)
                logger.info(f"Agent unsubscribed from channel: {channel}")
            except ValueError:
                logger.warning(f"Callback not found in {channel}")


    async def handle_event(self,channel:str,payload:str):
        """hanfles the incoming notify events.
        payload is json -> {'channel':'name', 'data':{...}}
        """
        try:
            data =json.loads(payload)

            if channel in self.subscribers:
                event_type = data.get('event_type','unknown')
                logger.info(f"Event recieved on '{channel}': {event_type}")
                print(f"Event on '{channel}': {event_type}")
                
                tasks = []
                for callback in self.subscribers[channel]:
                    tasks.append(asyncio.create_task(callback(data)))

                await asyncio.gather(*tasks, return_exceptions=True)
            else:
                logger.warning(f"No subscribers for channel: {channel}")
        
        except json.JSONDecodeError as e:
            logger.error(f"❌ Invalid json payload: {e}")
            print(f"❌ Invalid json payload: {e}")

        except Exception as e:
            logger.error(f"❌ Error handling event: {e}")
            print(f"❌ Error handling event: {e}")

    async def start_listening(self):
        """continously listens to postgres notifications.
        each event is passed to handle_event()."""
        
        if self.running:
            logger.warning("EventRouter is already running..")
            return
        
        self.running=True
        # self.conn = await self.db.pg_pool.acquire()
        try:
            for channel in self.subscribers.keys():
                await self.db.listen_channel(
                    channel,
                    lambda payload,ch=channel: self.handle_event(ch,payload))
                logger.info("Event router listening for events..")
                print("Event router active....")

                while self.running:
                    await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"Event router error: {e}")
            print(f"Event router error: {e}")
            self.running =False
            raise
        finally:
            await self.cleanup()
    
    async def emit(self,channel:str, data:dict):
        """send notify events to the database, encoded as json"""
        try:
            payload = json.dumps(data)
            await self.db.notify(channel,payload)
            logger.info(f"Emitted -> {channel}: {data.get('event_type', 'unknown')}")
            print(f"Emitted -> {channel}: {data.get('event_type', 'unknown')}")
        except Exception as e:
            logger.error(f"❌ failed to emit event on {channel}: {e}")
            print(f"❌ failed to emit event: {e}")

    async def stop(self):
        """stops listening to all events and cleanup."""
        logger.info("stopping event router...")
        print("stopping event router....")
        self.running = False

        await asyncio.sleep(0.5) # give time for some pending tasks to complete

    async def cleanup(self):
        """cleanup all listener connections."""
        try:
            for channel in list(self.subscribers.keys()):
                await self.db.unlisten_channel(channel)
            logger.info("EventRouter cleanup complete")
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

    def get_active_channels(self)-> List[str]:
        """Returm list of all channels that are being used.."""
        return list(self.subscribers.keys())

    def get_subscriber_count(self,channel:str) -> int:
        """get the number of subscribers for a channel""" 
        return len(self.subscribers.get(channel,[]))       