from sqlalchemy import Boolean, Integer, String, create_engine
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from decouple import config

# url = config("POSTGRES_URL")
url = ""
sqlite_url = config("SQLITE_URL")

if url:
    engine = create_engine(url)

if sqlite_url:
    sqlite_engine = create_engine(sqlite_url)


class Base(DeclarativeBase):
    pass


class BatchLog(Base):
    __tablename__ = "batch_log"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    ticker: Mapped[str] = mapped_column(String(10))
    sub_id: Mapped[str] = mapped_column(String(256))
    batch_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    file_name: Mapped[str | None] = mapped_column(String(256), nullable=True)
    file_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    should_retry: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
