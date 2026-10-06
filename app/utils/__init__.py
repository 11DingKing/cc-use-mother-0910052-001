"""业务模块说明。"""

from app.utils.validators import (
    validate_stock_code,
    validate_time_range,
    validate_period,
    validate_positive_number,
    validate_list_not_empty,
    parse_date,
)
from app.utils.coverage import (
    merge_intervals,
    compute_missing_ranges,
    is_fully_covered,
)

__all__ = [
    "validate_stock_code",
    "validate_time_range",
    "validate_period",
    "validate_positive_number",
    "validate_list_not_empty",
    "parse_date",
    "merge_intervals",
    "compute_missing_ranges",
    "is_fully_covered",
]
