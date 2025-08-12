from datetime import datetime
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.crud.base import CRUDBase
from app.models import Reservation, User


# Создаем новый класс, унаследованный от CRUDBase.
class CRUDReservation(CRUDBase):

    async def get_reservations_at_the_same_time(
        self,
        # Добавляем звёздочку, чтобы обозначить, что все дальнейшие параметры
        # должны передаваться по ключу. Это позволит располагать
        # параметры со значением по умолч перед параметрами без таких значений.
        *,
        from_reserve: datetime,
        to_reserve: datetime,
        meetingroom_id: int,
        # Добавляем новый опциональный параметр - id объекта бронирования.
        reservation_id: Optional[int] = None,
        session: AsyncSession,
    ) -> list[Reservation]:
        """Получаем все бронирования в указанный период для переговорки."""
        select_stmt = select(Reservation).where(
            Reservation.meetingroom_id == meetingroom_id,
            from_reserve <= Reservation.to_reserve,
            to_reserve >= Reservation.from_reserve
        )
        # Если передан id бронирования...
        if reservation_id is not None:
            # ... то к выражению нужно добавить новое условие.
            select_stmt = select_stmt.where(
                # id искомых объектов не равны id обновляемого объекта.
                Reservation.id != reservation_id
            )
        result = await session.execute(select_stmt)
        return result.scalars().all()

    async def get_future_reservations_for_room(
        self,
        room_id: int,
        session: AsyncSession,
    ) -> list[Reservation]:
        """Получаем все будущие бронирования для переговорки."""
        select_stmt = select(Reservation).where(
            Reservation.meetingroom_id == room_id,
            Reservation.from_reserve > datetime.now()
        )
        result = await session.execute(select_stmt)
        return result.scalars().all()

    async def get_by_user(
        self,
        session: AsyncSession,
        user: User,
    ) -> list[Reservation]:
        """Получаем все бронирования пользователя."""
        select_stmt = select(Reservation).where(
            Reservation.user_id == user.id
        )
        result = await session.execute(select_stmt)
        return result.scalars().all()

    # Новый метод
    async def get_count_res_at_the_same_time(
            self,
            from_reserve: datetime,
            to_reserve: datetime,
            session: AsyncSession,
    ) -> list[dict[str, int]]:
        reservations = await session.execute(
            # Получаем количество бронирований переговорок за период
            select(Reservation.meetingroom_id,
                   func.count(Reservation.meetingroom_id)).where(
                Reservation.from_reserve >= from_reserve,
                Reservation.to_reserve <= to_reserve
            ).group_by(Reservation.meetingroom_id)
        )
        reservations = reservations.all()
        res = [
            {"meetingroom_id": room_id, "count": count}
            for room_id, count in reservations
        ]
        return res


reservation_crud = CRUDReservation(Reservation)
