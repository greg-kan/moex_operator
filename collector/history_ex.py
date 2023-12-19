"""Функции для получения данных об исторических дневных котировках."""
from typing import Iterable, Optional

import aiohttp

from aiomoex import client, request_helpers
from aiomoex.request_helpers import DEFAULT_BOARD, DEFAULT_ENGINE, DEFAULT_MARKET, SECURITIES


def make_url_ex(
    *,
    history: Optional[bool] = None,
    engine: Optional[str] = None,
    market: Optional[str] = None,
    board: Optional[str] = None,
    security: Optional[str] = None,
    ending: Optional[str] = None,
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
    # if security:
    url_parts.append(f"/securities")
    if ending:
        url_parts.append(f"/{ending}")
    url_parts.append(".json")
    return "".join(url_parts)


async def get_board_history_ex(
    session: aiohttp.ClientSession,
    security: str = '',
    start: Optional[str] = None,
    end: Optional[str] = None,
    columns: Optional[Iterable[str]] = ("BOARDID", "TRADEDATE", "CLOSE", "VOLUME", "VALUE"),  # , "SECID"
    # board: str = DEFAULT_BOARD,
    board: Optional[str] = None,
    market: str = DEFAULT_MARKET,
    engine: str = DEFAULT_ENGINE,
) -> client.Table:
    """Получить историю торгов для указанной бумаги в указанном режиме торгов за указанный интервал дат.

    Описание запроса - https://iss.moex.com/iss/reference/65

    :param session:
        Сессия http соединения.
    :param security:
        Тикер ценной бумаги.
    :param start:
        Дата вида ГГГГ-ММ-ДД. При отсутствии данные будут загружены с начала истории.
    :param end:
        Дата вида ГГГГ-ММ-ДД. При отсутствии данные будут загружены до конца истории.
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
        history=True, engine=engine, market=market, board=board, security=security,
    )
    table = "history"
    query = request_helpers.make_query(start=start, end=end, table=table, columns=columns)
    return await request_helpers.get_long_data(session, url, table, query)
