#!/usr/bin/env python3
"""Setup script for Dialogs RAG System v2.0"""

import subprocess
import sys
import os
from pathlib import Path

def install_requirements():
    """Install requirements from requirements.txt"""
    print("Installing requirements...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])
        print("✅ Requirements installed successfully!")
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install requirements: {e}")
        return False
    return True

def create_directories():
    """Create necessary directories"""
    print("Creating directories...")
    directories = [
        "data/input",
        "data/output", 
        "logs",
        "models"
    ]
    
    for directory in directories:
        Path(directory).mkdir(parents=True, exist_ok=True)
        print(f"✅ Created directory: {directory}")

def create_env_file():
    """Create .env file from template"""
    if not os.path.exists(".env"):
        print("Creating .env file from template...")
        try:
            with open("env.example", "r") as src:
                content = src.read()
            with open(".env", "w") as dst:
                dst.write(content)
            print("✅ Created .env file from template")
            print("⚠️  Please edit .env file and add your OpenAI API key")
        except Exception as e:
            print(f"❌ Failed to create .env file: {e}")
    else:
        print("✅ .env file already exists")

def main():
    """Main setup function"""
    print("🚀 Setting up Dialogs RAG System v2.0")
    print("=" * 50)
    
    # Install requirements
    if not install_requirements():
        print("❌ Setup failed at requirements installation")
        return False
    
    # Create directories
    create_directories()
    
    # Create .env file
    create_env_file()
    
    print("=" * 50)
    print("✅ Setup completed successfully!")
    print("\nNext steps:")
    print("1. Edit .env file and add your OpenAI API key")
    print("2. Run: python main.py --input data/input/dialogs.xlsx --dry-run")
    print("3. Or run: make test-pipeline")
    
    return True

if __name__ == "__main__":
    main()
