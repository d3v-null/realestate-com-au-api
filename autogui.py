#!/usr/bin/env python

from argparse import ArgumentParser
from dataclasses import astuple
import random
import re
import requests
import pyautogui
import pywinctl
import time
import sys
from sys import stderr
import json
import subprocess
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from random import shuffle

from realestate_com_au import RealestateComAu
from realestate_com_au.objects.property import FullAddress, Property


def eprint(*args, **kwargs):
    print(*args, file=stderr, **kwargs)


def typewritefix(text, args):
    if args.debug:
        print(f"typewritefix: {text}")
    if sys.platform == "darwin":
        cmd = f"""on run {{arg1}}
            set the clipboard to arg1 as string
            tell application "System Events"
                key code 9 using {{command down}}
                key code 36
            end tell
        end run"""
        if args.debug:
            print(cmd)
        proc = subprocess.Popen(['osascript', '-', text],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, encoding='utf8')
        ret, err = proc.communicate(cmd)
        proc.wait()
        time.sleep(args.hold_delay)
    else:
        pyautogui.typewrite(text)
        time.sleep(args.hold_delay)


def hotkeyfix(hold, press, args):
    if args.debug:
        print(f"hotkeyfix: {hold} p {press}")
    if sys.platform == "darwin":
        holds = f"using {{{' down, '.join(hold)} down}}" if hold else ""
        press_code = pyautogui._pyautogui_osx.keyboardMapping[press]
        cmd = f"""tell application "System Events"
            key code "{press_code}" {holds}
        end tell"""
        if args.debug:
            print(cmd)
        proc = subprocess.Popen(['osascript', '-'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, encoding='utf8')
        ret, err = proc.communicate(cmd)
        proc.wait()
        time.sleep(args.hold_delay)
    else:
        pyautogui.hotkey(*hold, press)
        # with pyautogui.hold(hold):
        #     time.sleep(args.hold_delay)
        #     for key in press:
        #         pyautogui.keyDown(key)
        #         time.sleep(args.hold_delay)
        #         pyautogui.keyUp(key)


def browser_new_incognito(args):
    hold = ["ctrl", "shift"]
    if sys.platform == "darwin":
        hold = ["command", "shift"]
    press = 'n'  # chrome
    if "firefox" in args.browser.lower():
        press = 'p'
    hotkeyfix(hold, press, args)


def browser_new_tab(args):
    hold = ["ctrl"]
    if sys.platform == "darwin":
        hold = ["command"]
    hotkeyfix(hold, 't', args)


def browser_close_window(args):
    hold = ["ctrl", "shift"]
    if sys.platform == "darwin":
        hold = ["command", "shift"]
    hotkeyfix(hold, 'w', args)


def browser_focus_addressbar(args):
    hold = ["ctrl"]
    if sys.platform == "darwin":
        hold = ["command"]
    hotkeyfix(hold, 'l', args)


def browser_focus_console(args):
    """
    I can't get any key combo to work in Chrome on macOS Monterey
    """
    press = 'j'  # chrome
    if "firefox" in args.browser.lower():
        press = 'k'
    hold = ["ctrl", "shift"]
    if sys.platform == "darwin":
        hold = ["command", "option"]
    hotkeyfix(hold, press, args)


def brewser_select_all_clear(args):
    hold = ["ctrl"]
    if sys.platform == "darwin":
        hold = ["command"]
    hotkeyfix(hold, 'a', args)
    hotkeyfix([], 'backspace', args)


class CheeseIt(Exception):
    """
    we've been had
    """
    pass


def get_parser():
    parser = ArgumentParser(
        description="navigate to urls in incognito session, run scripts")
    parser.add_argument("--browser", type=str, default="Google Chrome",
                        help="Full name of browser: Google Chrome, firefox, Safari")
    parser.add_argument("--no-incognito", default=False, action="store_true",
                        help="use incognito mode")
    parser.add_argument("--fresh", action="store_true", default=False,
                        help="kill existing incognito windows and start new one")
    parser.add_argument("--close", action="store_true", default=False,
                        help="close window when done")
    parser.add_argument("--sql_url", type=str,
                        help="url for sqlalchemy.create_engine")
    parser.add_argument("--debug", action="store_true", default=False,
                        help="debug logs")
    parser.add_argument("--hold_delay", default=0.1,
                        help="time between holding keys and pressing keys")
    parser.add_argument("--query_fill", default="actual_id",
                        help="empty attribute to fill")
    # parser.add_argument("--urlscripts", type=str,
    #                     help="json file mapping urls and scripts to run")
    parser.add_argument("--url", type=str, help="url to navigate to")
    parser.add_argument("--urls", type=str, help="file containing urls to navigate to")
    parser.add_argument("--script", type=str, help="script to run")
    # example: "(new WebSocket('wss://localhost.local:42069')).onopen=(e)=>{e.target.send(JSON.stringify(_.pick(REA,['avmData','bathrooms','bedrooms', 'canonicalUrl','carSpaces','channel','floorArea','landArea','lat','lon','longStreetAddress','offMarket','postcode','propertyId','propertyType','propertyListing','propertyMarketTrends','propertyTimeline','state','suburb','yearBuilt'])))}"
    return parser


def get_window_menu_items(window, args):
    if sys.platform == "darwin":
        cmd = f"""
            set output to ""
            tell application "System Events"
                -- set frontmostProcess to name of first process where it is frontmost
                set frontmostProcess to name of first process whose name contains "{args.browser}"
                tell process frontmostProcess
                    set menuBarItems to menu items of menu 1 of menu bar item "Window" of menu bar 1
                    repeat with mbi in menuBarItems
                        if (name of mbi is not missing value) then
                            set output to output & ((name of mbi) as string) & linefeed
                        end if
                    end repeat
                end tell
            end tell
            do shell script "echo " & quoted form of output
        """
        if args.debug:
            print(cmd)
        proc = subprocess.Popen(['osascript', '-'],
                                stdin=subprocess.PIPE, stdout=subprocess.PIPE, encoding='utf8')
        ret, err = proc.communicate(cmd)
        proc.wait()
        return ret
    else:
        return window.title
    # menu_items = [*window.menu.getMenu()['Window']['entries'].keys()]
    # zoom_idx = menu_items.index('Zoom') or 0
    # return menu_items[(zoom_idx+1):]


def run_script_on_url(url, window, args, first=False):
    # watch for window title changes
    # old_handle = window.getHandle()
    old_menu_titles = get_window_menu_items(window, args)
    # old_menu = window.
    start_time = time.time()

    # navigate to url
    browser_focus_addressbar(args)
    typewritefix(url + '\r', args)
    if first:
        time.sleep(2)

    while get_window_menu_items(window, args) == old_menu_titles:
        if time.time() - start_time > 10:
            eprint(
                f"timed out waiting for {url} to change from {old_menu_titles}")
            raise CheeseIt("They're on to us")
        time.sleep(1)
    time.sleep(2)

    # run script
    browser_focus_console(args)
    if first:
        time.sleep(2)
        brewser_select_all_clear(args)
        typewritefix(args.script + '\r', args)
        time.sleep(2)
    else:
        hotkeyfix([], 'up', args)
        hotkeyfix([], '\r', args)
        time.sleep(2)

    if args.close:
        browser_close_window(args)


def main():
    parser = get_parser()
    args = parser.parse_args()
    # print(pywinctl.getAllWindows())

    app_names = [*map(str.lower, pywinctl.getAllAppsNames())]
    assert args.browser in app_names, f"browser {args.browser} not found in {app_names}"
    app_names = pywinctl.getAppsWithName(args.browser)
    assert len(app_names) == 1, f"multiple apps found matching {args.browser}"
    app_name = app_names[0].lower()
    windows = pywinctl.getAllWindows()

    pyautogui.FAILSAFE = False

    # if fresh: kill all incognito windows, find non-incognito window
    # else: find an incognito window
    main_window = None
    # incognito_window = None
    for win in windows:
        handle = win.getHandle()
        title = win.title
        # if args.debug:
        print(f"{handle}, {title} a:{win.isActive}")
        if app_name not in win.getAppName().lower():
            continue
        elif args.fresh:
            # kill existing incognito windows
            win.activate(wait=True)
            if main_window is None:
                main_window = win
            else:
                browser_close_window(args)
            continue
        elif main_window is None or win.isActive:
            main_window = win
            break

    # focus incognito window
    if not main_window.isActive:
        # print("not activating main window because debug")
        main_window.activate(wait=True)

    browser_new_incognito(args)
    time.sleep(1)
    window = pywinctl.getActiveWindow()

    engine = create_engine(
        args.sql_url,
        echo=args.debug,
        future=True
    )
    first=True
    with Session(engine) as session:
        if args.url:
            print("processing", args.url)
            run_script_on_url(args.url, window, args, first=first)
            exit(0)
        elif args.urls:
            with open(args.urls, 'r') as f:
                urls = f.read().splitlines()
                count = len(urls)
                for i, url in enumerate(urls):
                    if not url:
                        continue
                    print(f"processing url {i} / {count} : {url}")
                    p = session.query(Property).filter(Property.url == url).all()
                    if len(p):
                        print(f"- skipping (already in db)")
                        continue
                    run_script_on_url(url, window, args, first)
                    first=False
                if first:
                    exit(69)
        elif args.query_fill == "actual_id":
            ps = session.query(Property).filter(
                # Property.actual_id.is_(None)
                # Property.actual_id == -1
                Property.suburb == "Bentley"
            ).all()
            count = len(ps)
            api = RealestateComAu()
            if not len(ps):
                exit(69)
            shuffle(ps)
            for i, p in enumerate(ps):
                print(f"processing property {i} / {count} #({p.id}|{p.actual_id}) : {p.address}")
                any = False
                suggestions = api.suggestions(p.address)
                for suggestion in suggestions:
                    result = suggestion['source']
                    if result['postcode'] == p.address.postcode:
                        continue
                    any = True
                    print(" - ", result['shortAddress'])
                    if result['streetName'].split(' ')[:-1] != p.address.street_name.split(' ')[:-1]:
                        eprint(
                            f"  - warning: street name {result['streetName']} != {p.address.street_name}")
                        continue
                    if result['streetNumber'] != p.address.street_number:
                        eprint(
                            f"  - warning: street number {result['streetNumber']} != {p.address.street_number}")
                    else:
                        if args.debug:
                            print(f"  - perfect match")
                    url = result['url']
                    try:
                        run_script_on_url(url, window, args, first)
                        first = False
                    except CheeseIt:
                        browser_close_window(args)
                        browser_new_incognito(args)
                        time.sleep(1)
                        continue

                # retrieve updated p from db
                session.expire(p)
                p = session.get(Property, p.id)
                if not p.url:
                    if not any:
                        eprint(f"  - no suggestions")
                        p.actual_id = -7
                    else:
                        eprint(f"  - no perfect")
                        p.actual_id = -6
                    session.merge(p)
                    session.commit()
                    continue
                first = False
        else:
            ps = session.query(Property).filter(
                Property.url and Property.url != "").all()
            print(f"total properties with url: {len(ps)}")
            ps = [p for p in ps if not p.__getattribute__(args.query_fill)]
            print(f"total properties without {args.query_fill} {len(ps)}")
            count = len(ps)
            first = True
            for i, p in enumerate(ps):
                print(f"processing {i} / {count}")
                try:
                    run_script_on_url(p.url, window, args, first)
                except CheeseIt:
                    browser_close_window(args)
                    browser_new_incognito(args)
                    time.sleep(1)
                    continue
                first = False


if __name__ == "__main__":
    main()
