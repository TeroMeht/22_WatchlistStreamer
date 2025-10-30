import asyncio
import logging
from collections import deque
from typing import Callable, List, Dict


class BarBuffer:
    def __init__(self, batch_size: int):
        """
        :param batch_size: flush to DB when this many bars are collected
        """
        self.buffer = deque()
        self.lock = asyncio.Lock()
        self.batch_size = batch_size

    async def add(self, bar_data: dict, insert_func: Callable[[List[Dict], dict], None], database_config: dict):
        """
        Add a bar to the buffer; flush automatically when full.

        :param bar_data: dict containing bar info
        :param insert_func: async function that inserts bars into DB
        :param database_config: dictionary with DB connection info
        """
        async with self.lock:
            self.buffer.append(bar_data)
            if len(self.buffer) >= self.batch_size:
                await self._flush(insert_func, database_config)

    async def _flush(self, insert_func: Callable[[List[Dict], dict], None], database_config: dict):
        """Flush current buffer to database."""
        if not self.buffer:
            return

        batch = list(self.buffer)
        self.buffer.clear()

        try:
            await insert_func(batch, database_config)
        except Exception as e:
            logging.exception(f"Error during bulk insert: {e}")
            # Requeue failed batch
            async with self.lock:
                for item in batch:
                    self.buffer.appendleft(item)

    async def flush_remaining(self, insert_func: Callable[[List[Dict], dict], None], database_config: dict):
        """Manually flush any remaining bars (e.g., on shutdown)."""
        async with self.lock:
            await self._flush(insert_func, database_config)
