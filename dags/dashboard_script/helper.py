import dashboard_script.bot_telegram


def telegram_msg(msg='', markdown=0, conf=None):
    sent = 0
    while sent == 0 and conf:
        try:
            if markdown == 0:
                dashboard_script.bot_telegram.send(messages=[msg], timeout=15, conf_custom=conf)
            else:
                dashboard_script.bot_telegram.send(messages=[msg], timeout=15, parse_mode='markdown', conf_custom=conf)
            sent = 1
        except Exception as e:
            print(e)
            sent = 0


def telegram_image(src=None, caption=None, conf=None):
    sent = 0
    while sent == 0 and conf:
        try:
            with open(src, 'rb') as f:
                dashboard_script.bot_telegram.send(images=[f], captions=[caption], timeout=15, conf_custom=conf)
            sent = 1
        except Exception as e:
            print(e)
            sent = 0


def telegram_file(src=None, caption=None, conf=None):
    sent = 0
    while sent == 0 and conf:
        try:
            with open(src, 'rb') as f:
                dashboard_script.bot_telegram.send(files=[f], captions=[caption], timeout=15, conf_custom=conf)
            sent = 1
        except Exception as e:
            print(e)
            sent = 0
