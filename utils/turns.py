#!/usr/bin/env python3
"""
Утилиты для разбора диалогов на реплики
"""

import re

# Регулярное выражение для разбора ролей
ROLE_RX = re.compile(r"^(клиент|оператор)\s*:\s*(.*)$", re.IGNORECASE)


def split_turns(raw: str):
    """Разбор диалога на реплики с ролями"""
    turns = []
    for line in raw.splitlines():
        line = line.strip()
        if not line: 
            continue
        m = ROLE_RX.match(line)
        if m:
            turns.append({
                "speaker": m.group(1).lower(), 
                "text": m.group(2).strip()
            })
        elif turns:
            # Продолжение предыдущей реплики
            turns[-1]["text"] += " " + line
    return turns
