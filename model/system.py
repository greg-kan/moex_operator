import db_helper as dbh
from datetime import datetime
from logger import Logger
import settings as st

logger = Logger('system', st.APPLICATION_LOG, write_to_stdout=st.DEBUG_MODE).get()

SESSION_DB_TABLE = 'sys.session'
SESSION_STORED_PROC = 'sys.f_set_session'


class Session:
    def __init__(self):
        self.class_name = self.__class__.__name__
        self.number: int = 0
        self.time: datetime | None = None
        self.db_table: str = SESSION_DB_TABLE
        self.stored_proc: str = SESSION_STORED_PROC
        self._set()

    def _set(self):
        result = dbh.exec_sp_wo_params(SESSION_STORED_PROC)

        if result is None:
            logger.error(f"{self.class_name}._set(): DB Error occurred")
        else:
            logger.info(f"{self.class_name}._set(): Session was set, session: {result}")

            self.number = (str(result).split(',', 2)[0])[1:]
            self.time = datetime.strptime((str(result).split(',', 2)[1])[1:-2],
                                          '%Y-%m-%d %H:%M:%S.%f')

    def get_number(self) -> int | None:
        return self.number

    def get_time(self) -> datetime | None:
        return self.time
