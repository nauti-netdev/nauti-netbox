#      Copyright (C) 2020  Jeremy Schulman
#
#      This program is free software: you can redistribute it and/or modify
#      it under the terms of the GNU General Public License as published by
#      the Free Software Foundation, either version 3 of the License, or
#      (at your option) any later version.
#
#      This program is distributed in the hope that it will be useful,
#      but WITHOUT ANY WARRANTY; without even the implied warranty of
#      MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#      GNU General Public License for more details.
#
#      You should have received a copy of the GNU General Public License
#      along with this program.  If not, see <https://www.gnu.org/licenses/>.

# -----------------------------------------------------------------------------
# System Imports
# -----------------------------------------------------------------------------

from typing import Optional, Dict, List, Callable
import asyncio
from os import environ
from operator import itemgetter
from itertools import chain, starmap

# -----------------------------------------------------------------------------
# Public Imports
# -----------------------------------------------------------------------------

from httpx import AsyncClient


# -----------------------------------------------------------------------------
# Exports
# -----------------------------------------------------------------------------

__all__ = ["NetboxClient"]


# -----------------------------------------------------------------------------
#
#                              CODE BEGINS
#
# -----------------------------------------------------------------------------

class NetboxClient(AsyncClient):
    ENV_VARS = ["NETBOX_ADDR", "NETBOX_TOKEN"]
    DEFAULT_PAGE_SZ = 100
    API_RATE_LIMIT = 100

    def __init__(self, base_url=None, token=None, **clientopts):
        try:
            url = base_url or environ['NETBOX_ADDR']
            token = token or environ['NETBOX_TOKEN']
        except KeyError as exc:
            raise RuntimeError(f"Missing environment variable: {exc.args[0]}")

        super().__init__(
            base_url=f"{url}/api",
            headers=dict(Authorization=f"Token {token}"),
            verify=False,
            **clientopts
        )
        self._api_s4 = asyncio.Semaphore(self.API_RATE_LIMIT)

    async def request(self, *vargs, **kwargs):
        async with self._api_s4:
            return await super(NetboxClient, self).request(*vargs, **kwargs)

    async def paginate(
        self, url: str, page_sz: Optional[int] = None, filters: Optional[Dict] = None
    ) -> List[Dict]:
        """
        Concurrently paginate GET on url for the given page_sz and optional
        Caller filters (Netbox API specific).  Return the list of all page
        results.

        Parameters
        ----------
        url:
            The Netbox API URL endpoint

        page_sz:
            Max number of result items

        filters:
            The Netbox API params filter options.

        Returns
        -------
        List of all Netbox API results from all pages
        """

        # GET the url for limit = 1 record just to determin the total number of
        # items.

        params = filters or {}
        params["limit"] = 1

        res = await self.get(url, params=params)
        res.raise_for_status()
        body = res.json()
        count = body["count"]

        # create a list of tasks to run concurrently to fetch the data in pages.
        # NOTE: that we _MUST_ do a params.copy() to ensure that each task has a
        # unique offset count.  Observed that if copy not used then all tasks have
        # the same (last) value.

        params["limit"] = page_sz or self.DEFAULT_PAGE_SZ
        tasks = list()

        for offset in range(0, count, params["limit"]):
            params["offset"] = offset
            tasks.append(self.get(url, params=params.copy()))

        task_results = await asyncio.gather(*tasks)

        # return the flattened list of results

        return list(
            chain.from_iterable(task_r.json()["results"] for task_r in task_results)
        )

    # -------------------------------------------------------------------------
    #
    #                        Device Helper Methods
    #
    # -------------------------------------------------------------------------

    async def fetch_device(self, hostname):
        res = await self.get("/dcim/devices/", params=dict(name=hostname))
        res.raise_for_status()
        body = res.json()
        return [] if not body["count"] else body["results"]

    async def fetch_devices(self, hostname_list, key=None):
        res = await asyncio.gather(*(map(self.fetch_device, hostname_list)))
        flat = chain.from_iterable(res)
        if not key:
            return list(flat)

        key_fn = key if isinstance(key, Callable) else itemgetter(key)
        return {key_fn(rec): rec for rec in flat}

    # -------------------------------------------------------------------------
    #
    #                        Device Interface Helper Methods
    #
    # -------------------------------------------------------------------------

    async def fetch_device_interface(self, hostname, if_name):
        res = await self.get(
            "/dcim/interfaces/", params=dict(device=hostname, name=if_name)
        )
        res.raise_for_status()
        body = res.json()
        return [] if not body["count"] else body["results"]

    async def fetch_devices_interfaces(self, items, key=None):
        res = await asyncio.gather(*(starmap(self.fetch_device_interface, items)))
        flat = chain.from_iterable(res)
        if not key:
            return list(flat)

        key_fn = key if isinstance(key, Callable) else itemgetter(key)  # noqa
        return {key_fn(rec): rec for rec in flat}
