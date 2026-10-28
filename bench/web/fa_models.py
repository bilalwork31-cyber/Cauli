"""SQLAlchemy model for web_job, shared by the FastAPI app and its tasks."""

from sqlalchemy import Integer, Text
from sqlalchemy.orm import Mapped, mapped_column

from sqla_models import ASYNC_PG_DSN, Base

__all__ = ["ASYNC_PG_DSN", "WebJob"]


class WebJob(Base):
    __tablename__ = "web_job"

    id: Mapped[int] = mapped_column(primary_key=True)
    payload: Mapped[str] = mapped_column(Text)
    processed: Mapped[int] = mapped_column(Integer, default=0)
