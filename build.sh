#!/usr/bin/env bash

# Встановлюємо всі залежності з requirements.txt
pip install -r requirements.txt

# ВАЖЛИВО:
# Встановлюємо Playwright-браузери ТА їхні системні залежності
playwright install --with-deps chromium