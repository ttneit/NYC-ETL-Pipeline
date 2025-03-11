from dagster import Definitions, load_assets_from_modules

from .assets import raw,bronze,silver,gold,platinum
all_assets = load_assets_from_modules([raw,bronze,silver,gold,platinum])

defs = Definitions(
    assets=all_assets,
)
