from playwright.sync_api import sync_playwright
import nest_asyncio


def wine_spectator_australia():
    """
    extracts winery names from
    https://www.winespectator.com/articles/alphabetical-guide-australia-wine-123123
    """

    nest_asyncio.apply()
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=False)
    page = browser.new_page()
    table = page.locator("table tr").all_inner_texts()
    browser.close()
    no_empty_cells = [x for x in table if x]
    wineries = [x for x in no_empty_cells if "$" not in x]

    return wineries
