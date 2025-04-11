import asyncio
from pyppeteer import launch
from pyppeteer.launcher import connect

DEFAULT_ARGS = [
    # '--disable-background-networking',
    # '--disable-background-timer-throttling',
    # '--disable-breakpad',
    # '--disable-browser-side-navigation',
    # '--disable-client-side-phishing-detection',
    # '--disable-default-apps',
    # '--disable-dev-shm-usage',
    # '--disable-extensions',
    # '--disable-features=site-per-process',
    # '--disable-hang-monitor',
    # '--disable-popup-blocking',
    # '--disable-prompt-on-repost',
    # '--disable-sync',
    # '--disable-translate',
    # '--metrics-recording-only',
    # '--no-first-run',
    # '--safebrowsing-disable-auto-update',
]

AUTOMATION_ARGS = [
    # '--enable-automation',
    # '--password-store=basic',
    # '--use-mock-keychain',
]


async def main():
    # udd = "/Users/dev/Library/Application\ Support/Google/Chrome/Default"
    # args = [
    #     "--proxy-server=http://localhost:8080"
    #     # f"--user-data-dir={udd}"
    # ] + DEFAULT_ARGS + AUTOMATION_ARGS
    # browser = await launch(
    #     headless=False, devtools=True, autoClose=False, args=args, ignoreDefaultArgs=True,
    #     userDataDir=udd
    # )
    browser = await connect(browserURL='http://127.0.0.1:9222')
    context = await browser.createIncognitoBrowserContext()
    page = await context.newPage()
    page.setDefaultNavigationTimeout(0)
    await page.goto('https://www.realestate.com.au/property/')
    # el = await page.querySelector('#krux_tag_container')
    breakpoint()
    # await page.goto('https://www.realestate.com.au/property/4-asteroid-way-carlisle-wa-6101?pid=p4ep-pdp|sold-pdp:property-history-cta#timeline')
    # await page.screenshot({'path': 'example.png'})
    # await browser.close()

asyncio.get_event_loop().run_until_complete(main())
