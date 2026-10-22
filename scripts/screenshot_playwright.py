"""Runs inside the Playwright container: screenshots the Streamlit dashboard running on the host."""
from playwright.sync_api import sync_playwright

URL = "http://host.docker.internal:8501"

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page(viewport={"width": 1600, "height": 1500})
    page.goto(URL)
    page.wait_for_selector("text=Events processed", timeout=60000)
    page.wait_for_timeout(8000)  # let the charts finish drawing
    page.screenshot(path="/out/dashboard.png", full_page=True)
    browser.close()
print("saved")
