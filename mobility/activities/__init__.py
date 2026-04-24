from .activity import Activity, ActivityParameters
from .home import HomeActivity
from .leisure import LeisureActivity
from .other import OtherActivity
from .shopping import ShopActivity
from .studies import StudyActivity
from .work import WorkActivity

Home = HomeActivity
Leisure = LeisureActivity
Other = OtherActivity
Shop = ShopActivity
Study = StudyActivity
Work = WorkActivity

__all__ = [
    "Activity",
    "ActivityParameters",
    "Home",
    "HomeActivity",
    "Leisure",
    "LeisureActivity",
    "Other",
    "OtherActivity",
    "Shop",
    "ShopActivity",
    "Study",
    "StudyActivity",
    "Work",
    "WorkActivity",
]
