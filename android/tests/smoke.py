"""Exercise the installed offline planner through its launch activity."""
import re
import os
import subprocess
import time
from xml.etree import ElementTree


def adb(*args):
    return subprocess.check_output(['adb', *args], text=True, timeout=90)


def wait_for(label, *, click=False):
    deadline = time.monotonic() + 150
    while time.monotonic() < deadline:
        try:
            adb('shell', 'uiautomator', 'dump', '/sdcard/aycf-screen.xml')
            nodes = ElementTree.fromstring(adb('shell', 'cat', '/sdcard/aycf-screen.xml')).iter('node')
            for node in nodes:
                text = (node.get('text') or node.get('content-desc') or '').casefold()
                if label.casefold() != text:
                    continue
                if click:
                    x, y, right, bottom = map(int, re.findall(r'\d+', node.attrib['bounds']))
                    adb('shell', 'input', 'tap', str((x + right) // 2), str((y + bottom) // 2))
                return
        except (subprocess.SubprocessError, ElementTree.ParseError):
            pass
        time.sleep(1)
    raise AssertionError('Android did not show ' + label)


adb('shell', 'input', 'keyevent', '82')
adb('shell', 'am', 'start', '-n', os.environ.get('AYCF_PACKAGE', 'uk.aycf.mobile') + '/uk.aycf.mobile.MainActivity')
wait_for('Start fresh', click=True)
wait_for('Start fresh', click=True)
wait_for('Wizz setup needed')
wait_for('Menu', click=True)
wait_for('Scan status', click=True)
wait_for('System status')
wait_for('Menu', click=True)
wait_for('Manchester connections', click=True)
wait_for('No matching flights in this scan')
wait_for('Menu', click=True)
wait_for('Scan status', click=True)
wait_for('System status')
wait_for('Menu', click=True)
wait_for('Wizz account', click=True)
wait_for('Connect your Wizz account')
wait_for('Save and connect', click=True)
wait_for('Enter both your Wizz email and password.')
adb('shell', 'input', 'keyevent', '4')
wait_for('System status')
print('Fresh install, offline planner and scan status passed.')

# A phone without a network must still give the user a usable sign-in screen.
adb('shell', 'svc', 'wifi', 'disable')
adb('shell', 'svc', 'data', 'disable')
try:
    wait_for('Menu', click=True)
    wait_for('Wizz account', click=True)
    wait_for('Open Wizz sign-in', click=True)
    wait_for('Connect wallet', click=True)
    wait_for('Open Wizz wallet')
    adb('shell', 'input', 'keyevent', '4')
    wait_for('Wizz account')
    adb('shell', 'input', 'keyevent', '4')
    wait_for('System status')
    # Save fixture credentials with all device networks disabled, then kill/reopen
    # the process to verify encrypted persistence without contacting Wizz.
    wait_for('Menu', click=True)
    wait_for('Wizz account', click=True)
    wait_for('Wizz email', click=True)
    adb('shell', 'input', 'text', 'fixture@example.com')
    wait_for('Wizz password', click=True)
    adb('shell', 'input', 'text', 'test-only-offline')
    adb('shell', 'input', 'keyevent', '4')
    wait_for('Save and connect', click=True)
    wait_for('fixture@example.com\nLogin saved securely')
    package = os.environ.get('AYCF_PACKAGE', 'uk.aycf.mobile')
    adb('shell', 'am', 'force-stop', package)
    adb('shell', 'am', 'start', '-n', package + '/uk.aycf.mobile.MainActivity')
    wait_for('Menu', click=True)
    wait_for('Wizz account', click=True)
    wait_for('fixture@example.com\nLogin saved securely')
    wait_for('Reconnect with saved login')
    wait_for('Forget saved login', click=True)
    wait_for('Forget', click=True)
    wait_for('Saved email and password removed.')
    print('Saved login survived a cold restart without password re-entry.')
finally:
    adb('shell', 'svc', 'wifi', 'enable')
    adb('shell', 'svc', 'data', 'enable')
print('Offline sign-in recovery and return to the planner passed.')
