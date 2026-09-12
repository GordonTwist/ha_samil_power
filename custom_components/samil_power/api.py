"""Samil Power API Client."""

from __future__ import annotations

import asyncio
import logging
import socket
import time
from typing import Any, Dict, List, Tuple

import async_timeout

from .const import LOGGER

# Import the necessary modules from the samil package
from samil.inverter import InverterFinder, KeepAliveInverter, InverterNotFoundError
from samil.inverterutil import connect_inverters


class SamilPowerApiClientError(Exception):
    """Exception to indicate a general API error."""


class SamilPowerApiClientCommunicationError(
    SamilPowerApiClientError,
):
    """Exception to indicate a communication error."""


class SamilPowerApiClientAuthenticationError(
    SamilPowerApiClientError,
):
    """Exception to indicate an authentication error."""


class SamilPowerApiClient:
    """Samil Power API Client."""

    def __init__(
        self,
        interface: str = "",
        inverters: int = 1,
    ) -> None:
        """Initialize the Samil Power API Client."""
        self._interface = interface
        self._inverters_count = int(inverters)  # Ensure this is an integer
        self._inverters: Dict[int, KeepAliveInverter] = {}
        self._model_info = {}
        self._serial_to_index: Dict[str, int] = {}
        self._connected = False
        self._reconnect_backoff_seconds = 30.0
        self._next_reconnect_attempt = 0.0

    async def async_connect(self, force: bool = False) -> None:
        """Connect to the inverters."""
        if self._connected and not force:
            return

        try:
            LOGGER.info(f"Attempting to connect to inverters with interface={self._interface}, count={self._inverters_count}")
            
            # Run the connection in a separate thread to avoid blocking
            loop = asyncio.get_event_loop()
            discovered_inverters = await loop.run_in_executor(
                None, self._connect_inverters
            )
            for inverter in discovered_inverters:
                model_info = await loop.run_in_executor(None, inverter.model)
                serial_number = model_info.get("serial_number")
                inverter_index = self._serial_to_index.get(serial_number)
                if inverter_index is None:
                    inverter_index = self._allocate_inverter_index()
                    if serial_number:
                        self._serial_to_index[serial_number] = inverter_index

                previous_inverter = self._inverters.get(inverter_index)
                if previous_inverter and previous_inverter is not inverter:
                    try:
                        previous_inverter.disconnect()
                    except Exception:  # pylint: disable=broad-except
                        pass

                self._inverters[inverter_index] = inverter
                self._model_info[inverter_index] = model_info
                LOGGER.info(
                    "Inverter %s model info: %s, SN: %s",
                    inverter_index,
                    model_info.get("model_name", "Unknown"),
                    serial_number or "Unknown",
                )

            self._connected = bool(self._inverters)
            if self._connected:
                self._next_reconnect_attempt = 0.0

            LOGGER.info(
                "Connected to %s/%s configured inverters",
                len(self._inverters),
                self._inverters_count,
            )
                
        except InverterNotFoundError as exception:
            msg = f"No inverters found - {exception}"
            LOGGER.error(msg)
            raise SamilPowerApiClientCommunicationError(msg) from exception
        except Exception as exception:  # pylint: disable=broad-except
            msg = f"Error connecting to inverters - {exception}"
            LOGGER.error(msg)
            raise SamilPowerApiClientError(msg) from exception

    def _connect_inverters(self):
        """Connect to the inverters (runs in executor)."""
        inverters = []
        try:
            LOGGER.debug(f"Starting inverter connection with interface={self._interface}, count={self._inverters_count}")
            # First try with the specified interface
            finder = InverterFinder(interface_ip=self._interface)
            finder.open()
            try:
                # Find each inverter directly
                for i in range(self._inverters_count):
                    LOGGER.debug(f"Finding inverter {i} with interface {self._interface}")
                    try:
                        sock, addr = finder.find_inverter()
                        LOGGER.info(f"Found inverter at address {addr}")
                        inverter = KeepAliveInverter(sock, addr)
                        inverters.append(inverter)
                    except Exception as e:
                        LOGGER.error(f"Error finding inverter {i}: {str(e)}")
                        if i == 0:  # If we can't find even the first inverter, re-raise
                            raise
            finally:
                finder.close()
        except Exception as e:
            # If there's an error with the specific interface, try with an empty interface (broadcast)
            if self._interface and not inverters:
                # Log that we're falling back to broadcast discovery
                LOGGER.info(f"Failed to connect using interface {self._interface}, trying broadcast discovery")
                # Try again with empty interface for broadcast
                finder = InverterFinder(interface_ip="")
                finder.open()
                try:
                    # Find each inverter directly
                    for i in range(self._inverters_count):
                        LOGGER.debug(f"Finding inverter {i} with broadcast discovery")
                        try:
                            sock, addr = finder.find_inverter()
                            LOGGER.info(f"Found inverter at address {addr} using broadcast discovery")
                            inverter = KeepAliveInverter(sock, addr)
                            inverters.append(inverter)
                        except Exception as e:
                            LOGGER.error(f"Error finding inverter {i} with broadcast: {str(e)}")
                            if i == 0:  # If we can't find even the first inverter, re-raise
                                raise
                finally:
                    finder.close()
            else:
                # Re-raise the original exception if we weren't using a specific interface
                # or if we already have some inverters
                LOGGER.error(f"Error during inverter connection: {str(e)}")
                raise
        
        if not inverters:
            raise InverterNotFoundError("No inverters found")
            
        return inverters

    async def async_get_data(self) -> Dict[int, Dict]:
        """Get data from the inverters."""
        if not self._connected:
            await self.async_connect()

        try:
            # Run the status requests in a separate thread to avoid blocking
            loop = asyncio.get_event_loop()
            
            # Get status for each inverter
            status_data = {}
            failed_inverters = []
            for i, inverter in list(self._inverters.items()):
                try:
                    status = await loop.run_in_executor(None, inverter.status)
                except Exception as exception:  # pylint: disable=broad-except
                    LOGGER.warning("Failed to get status for inverter %s: %s", i, exception)
                    failed_inverters.append(i)
                    self._mark_inverter_disconnected(i)
                    continue

                # Combine with model info
                combined_data = {
                    "model": self._model_info.get(i, {}),
                    "status": status
                }
                status_data[i] = combined_data

            should_rediscover = bool(failed_inverters) or len(self._inverters) < self._inverters_count
            if should_rediscover and self._should_retry_discovery():
                await self.async_connect(force=True)
                refresh_indexes = set(failed_inverters)
                refresh_indexes.update(set(self._inverters) - set(status_data))
                for i in refresh_indexes:
                    inverter = self._inverters.get(i)
                    if inverter is None:
                        continue
                    try:
                        status = await loop.run_in_executor(None, inverter.status)
                    except Exception as exception:  # pylint: disable=broad-except
                        LOGGER.warning("Reconnected inverter %s still unavailable: %s", i, exception)
                        self._mark_inverter_disconnected(i)
                        continue
                    status_data[i] = {
                        "model": self._model_info.get(i, {}),
                        "status": status,
                    }

            self._connected = bool(self._inverters)
            if not status_data:
                msg = "No inverter data available"
                raise SamilPowerApiClientError(msg)

            return status_data
            
        except Exception as exception:  # pylint: disable=broad-except
            self._connected = False  # Mark as disconnected on error
            msg = f"Error getting data from inverters - {exception}"
            raise SamilPowerApiClientError(msg) from exception

    def _allocate_inverter_index(self) -> int:
        """Allocate an index for a newly discovered inverter."""
        for i in range(self._inverters_count):
            if i not in self._inverters:
                return i
        return len(self._inverters)

    def _should_retry_discovery(self) -> bool:
        """Decide if reconnect discovery should be retried now."""
        now = time.monotonic()
        if now < self._next_reconnect_attempt:
            return False
        self._next_reconnect_attempt = now + self._reconnect_backoff_seconds
        return True

    def _mark_inverter_disconnected(self, inverter_index: int) -> None:
        """Disconnect and remove a failed inverter slot."""
        inverter = self._inverters.pop(inverter_index, None)
        if inverter is None:
            return
        try:
            inverter.disconnect()
        except Exception:  # pylint: disable=broad-except
            pass

    async def async_disconnect(self) -> None:
        """Disconnect from the inverters."""
        if not self._inverters:
            return
            
        for inverter in self._inverters.values():
            try:
                inverter.disconnect()
            except Exception:  # pylint: disable=broad-except
                pass
                
        self._inverters = {}
        self._connected = False
