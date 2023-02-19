import datetime
import json
import logging
import multiprocessing.dummy
import sys
import warnings
import zoneinfo
from typing import Any, Generator, Iterable

import click
import requests.adapters
import scrapelib
import tabulate
import tqdm

from .request_types import request_types

Interval = Iterable[tuple[datetime.datetime, datetime.datetime]]


class Downloader(scrapelib.Scraper):
    BASE_URL = "http://311api.cityofchicago.org/open311/v2/requests.json"

    def __init__(self, request_type=None):

        super().__init__(requests_per_minute=0, retry_attempts=5, retry_wait_seconds=10)
        self.timeout = 30

        adapter = requests.adapters.HTTPAdapter(pool_connections=100, pool_maxsize=100)
        self.mount("http://", adapter)

        if request_type:
            self.args = {
                "extensions": "true",
                "service_code": ",".join(
                    request_types[r_type]["service_code"] for r_type in request_type
                ),
            }
        else:
            self.args = {
                "extensions": "true",
            }

    def prepare_args(
        self, start: datetime.datetime, end: datetime.datetime, page_size: int
    ) -> dict[str, Any]:
        args = self.args.copy()
        args.update(
            {
                "start_date": start.isoformat(),
                "end_date": end.isoformat(),
                "page": 1,
                "page_size": page_size,
            }
        )
        return args

    def __call__(
        self, interval: tuple[datetime.datetime, datetime.datetime]
    ) -> list[dict[str, Any]]:
        start, end = interval
        results = []
        page_size = 200
        args = self.prepare_args(start, end, page_size)
        page = self.get(self.BASE_URL, params=args).json()
        results.extend(page)

        while len(page) == page_size:
            args["page"] += 1
            try:
                page = self.get(self.BASE_URL, params=args).json()
            except scrapelib.HTTPError as e:
                warnings.warn(
                    "Could not load {url}. We will miss some requests from {date}".format(
                        url=e.response.request.url, date=start.date()
                    )
                )
                page = []
            results.extend(page)

        return results

    def request(self, method, url, **kwargs):
        response = super().request(method, url, **kwargs)

        self._check_errors(response)

        return response

    def _check_errors(self, response):

        try:
            response.json()
        except json.decoder.JSONDecodeError:
            response.status_code = 500
            raise scrapelib.HTTPError(response)


def day_intervals(
    start_datetime: datetime.datetime, end_datetime: datetime.datetime
) -> Generator[tuple[datetime.datetime, datetime.datetime], None, None]:

    start = start_datetime
    while start < end_datetime:
        yield start, datetime.datetime.combine(start, datetime.time.max).replace(
            tzinfo=start.tzinfo
        )
        start += datetime.timedelta(days=1)


@click.command()
@click.option(
    "-s",
    "--start-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.datetime.today(),
    help="the first day of the time range to check",
)
@click.option(
    "-e",
    "--end-date",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.datetime.today(),
    help="the last day of the time range to check",
)
@click.option("-t", "--request-type", multiple=True, help="service types to fetch")
@click.option("-v", "--verbose", count=True, help="verbosity level")
@click.option(
    "--list-request-types", is_flag=True, default=False, help="list valid request types"
)
@click.option(
    "--save-json", is_flag=True, default=False, help="save output as json (datestamp as filename)"
)

def main(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    verbose: int,
    request_type,
    list_request_types,
    save_json,
) -> None:
    """Download service requests from the Chicago Open311 API. By
    default, today's requests of all types. Will write service
    requests as line-delimited JSON to stdout."""

    for r_type in request_type:
        if r_type not in request_types:
            raise click.BadParameter(
                f"{r_type} is not a valid request type. To see valid types run 'chicagorequests --list-request-types'"
            )

    if list_request_types:
        table = tabulate.tabulate(
            [(k, v["service_name"]) for k, v in request_types.items()],
            headers=["type", "definition"],
            maxcolwidths=40,
        )
        click.echo(table)
        sys.exit()

    if verbose >= 2:
        logging.basicConfig(level=logging.DEBUG)
    elif verbose == 1:
        logging.basicConfig(level=logging.INFO)

    start_datetime = datetime.datetime.combine(start_date, datetime.time.min).replace(
        tzinfo=zoneinfo.ZoneInfo("America/Chicago")
    )
    end_datetime = datetime.datetime.combine(end_date, datetime.time.max).replace(
        tzinfo=zoneinfo.ZoneInfo("America/Chicago")
    )

    intervals = day_intervals(start_datetime, end_datetime)

    downloader = Downloader(request_type=request_type)
    json_file = None
    if save_json:
        timestamp = int(datetime.datetime.now().timestamp())
        filename = f'chicagorequests_{timestamp}.json'
        print(f'Saving file as {filename}')
        json_file = open(filename, 'w', encoding='utf-8')

    with multiprocessing.dummy.Pool(15) as pool:
        for day in tqdm.tqdm(
            pool.imap_unordered(downloader, intervals),
            total=(end_datetime - start_datetime).days + 1,
            colour="#228b22",
            unit="day",
        ):
            for result in day:
                if json_file:
                    json.dump(result, json_file, ensure_ascii=False, indent=2)
                click.echo(json.dumps(result))
        if json_file:
            json_file.close()
