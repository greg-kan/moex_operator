"""Функции для получения данных об исторических дневных котировках."""
from typing import Iterable, Optional

import aiohttp

from aiomoex import client, request_helpers
from aiomoex.request_helpers import DEFAULT_BOARD, DEFAULT_ENGINE, DEFAULT_MARKET, SECURITIES


def make_url_ex(
    *,
    history: bool | None = None,
    engine: str | None = None,
    market: str | None = None,
    board: str | None = None,
    ending: str | None = None,
) -> str:
    """Формирует URL для запроса."""
    url_parts = ["https://iss.moex.com/iss"]
    if history:
        url_parts.append("/history")
    if engine:
        url_parts.append(f"/engines/{engine}")
    if market:
        url_parts.append(f"/markets/{market}")
    if board:
        url_parts.append(f"/boards/{board}")
    url_parts.append(f"/securities")
    if ending:
        url_parts.append(f"/{ending}")
    url_parts.append(".json")
    return "".join(url_parts)


async def get_board_history_all_securities_on_last_date(
    session: aiohttp.ClientSession,
    columns: Optional[Iterable[str]] = ("BOARDID", "TRADEDATE", "CLOSE", "VOLUME", "VALUE"),
    board: str | None = DEFAULT_BOARD,
    market: str = DEFAULT_MARKET,
    engine: str = DEFAULT_ENGINE,
) -> client.Table:
    """Получить историю торгов для всех бумаг в указанном режиме торгов за последнюю дату.

    Описание запроса - https://iss.moex.com/iss/reference/65 - TODO:ЭТО НЕ ТОЧНО

    :param session:
        Сессия http соединения.
    :param columns:
        Кортеж столбцов, которые нужно загрузить - по умолчанию режим торгов, дата торгов, цена закрытия
        и объем в штуках и стоимости. Если пустой или None, то загружаются все столбцы.
    :param board:
        Режим торгов - по умолчанию основной режим торгов T+2.
    :param market:
        Рынок - по умолчанию акции.
    :param engine:
        Движок - по умолчанию акции.

    :return:
        Список словарей, которые напрямую конвертируется в pandas.DataFrame.
    """
    url = make_url_ex(
        history=True,
        engine=engine,
        market=market,
        board=board,
    )

    table = "history"
    query = request_helpers.make_query(table=table, columns=columns)
    return await request_helpers.get_long_data(session, url, table, query)
