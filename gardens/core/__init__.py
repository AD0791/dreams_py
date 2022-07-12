from .agyw import AgywPrev


_datim = AgywPrev()
BASE = _datim.data_dreams_valid
GARDENING_SERVED = BASE[
    BASE.gardening == "yes"
]


WHO_AM_I = 'core of version  22 updated of agyw datim script'
