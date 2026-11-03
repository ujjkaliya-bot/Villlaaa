#!/bin/bash
# OTP Bot - VPS Startup Script

echo "Installing dependencies..."
pip3 install -r requirements.txt

echo "Starting bot..."
python3 bot.py
