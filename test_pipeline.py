#!/usr/bin/env python3
"""
Test script for Dialogs RAG pipeline
"""
import os
import sys
import subprocess
import pandas as pd

def test_pipeline():
    """Test the complete pipeline with sample data"""
    
    # Set environment variables
    os.environ["BATCH_ID"] = "test-2025-01-14"
    os.environ["N_DIALOGS"] = "5"
    os.environ["EXTRACT_MODE"] = "RULES"
    os.environ["DUCKDB_PATH"] = "data/test_rag.duckdb"
    os.environ["REQUIRE_QUALITY_PASS"] = "true"
    
    print("🧪 Testing Dialogs RAG Pipeline")
    print("=" * 50)
    
    try:
        # Test ingest
        print("1. Testing ingest...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.ingest_excel", 
            "--file", "data/input/sample_dialogs.xlsx", 
            "--batch", "test-2025-01-14"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Ingest failed: {result.stderr}")
            return False
        print(f"✅ Ingest: {result.stdout.strip()}")
        
        # Test extract
        print("2. Testing extract...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.extract_entities", 
            "--batch", "test-2025-01-14"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Extract failed: {result.stderr}")
            return False
        print(f"✅ Extract: {result.stdout.strip()}")
        
        # Test normalize
        print("3. Testing normalize...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.normalize", 
            "--batch", "test-2025-01-14"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Normalize failed: {result.stderr}")
            return False
        print(f"✅ Normalize: {result.stdout.strip()}")
        
        # Test dedup
        print("4. Testing dedup...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.dedup", 
            "--batch", "test-2025-01-14"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Dedup failed: {result.stderr}")
            return False
        print(f"✅ Dedup: {result.stdout.strip()}")
        
        # Test aggregate
        print("5. Testing aggregate...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.aggregate", 
            "--batch", "test-2025-01-14", 
            "--n_dialogs", "5"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Aggregate failed: {result.stderr}")
            return False
        print(f"✅ Aggregate: {result.stdout.strip()}")
        
        # Test quality
        print("6. Testing quality...")
        result = subprocess.run([
            sys.executable, "-m", "pipeline.quality", 
            "--batch", "test-2025-01-14", 
            "--n_dialogs", "5"
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Quality failed: {result.stderr}")
            return False
        print(f"✅ Quality: {result.stdout.strip()}")
        
        print("\n🎉 All tests passed! Pipeline is working correctly.")
        return True
        
    except Exception as e:
        print(f"❌ Test failed with exception: {e}")
        return False

if __name__ == "__main__":
    success = test_pipeline()
    sys.exit(0 if success else 1)

