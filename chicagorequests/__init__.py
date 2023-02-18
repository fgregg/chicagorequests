import datetime
import json
import multiprocessing.dummy
import zoneinfo
from typing import Any, Generator, Iterable

import click
import scrapelib
import tqdm

Interval = Iterable[tuple[datetime.datetime, datetime.datetime]]


class Downloader(scrapelib.Scraper):
    BASE_URL = "http://311api.cityofchicago.org/open311/v2/requests.json"

    @staticmethod
    def prepare_args(
        start: datetime.datetime, end: datetime.datetime
    ) -> dict[str, Any]:
        return {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
            "extensions": "true",
            "page": 1,
        }

    def __call__(
        self, interval: tuple[datetime.datetime, datetime.datetime]
    ) -> list[dict[str, Any]]:
        start, end = interval
        results = []
        page_size = 50
        args = self.prepare_args(start, end)
        page = self.get(self.BASE_URL, params=args).json()
        results.extend(page)

        while len(page) == page_size:
            args["page"] += 1
            page = self.get(self.BASE_URL, params=args).json()
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
    "--date-start",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.datetime.today(),
)
@click.option(
    "--date-end",
    type=click.DateTime(formats=["%Y-%m-%d"]),
    default=datetime.datetime.today(),
)
def main(date_start: datetime.datetime, date_end: datetime.datetime) -> None:
    start_datetime = datetime.datetime.combine(date_start, datetime.time.min).replace(
        tzinfo=zoneinfo.ZoneInfo("America/Chicago")
    )
    end_datetime = datetime.datetime.combine(date_end, datetime.time.max).replace(
        tzinfo=zoneinfo.ZoneInfo("America/Chicago")
    )

    intervals = day_intervals(start_datetime, end_datetime)

    downloader = Downloader(requests_per_minute=0, retry_attempts=3)

    with multiprocessing.dummy.Pool(15) as pool:
        for day in tqdm.tqdm(
            pool.imap_unordered(downloader, intervals),
            total=(end_datetime - start_datetime).days + 1,
            colour="green",
            unit="day",
        ):
            for result in day:
                click.echo(json.dumps(result))
