# 🚀 Deployment Guide - Dialogs RAG System v1.0.0

## 📦 Package Contents

```
dialogs-rag-v2/
├── src/                           # Source code
│   ├── config/                   # Configuration management
│   └── pipeline/                 # Processing modules
├── data/                         # Data directories
│   ├── input/                   # Place your dialogs here
│   └── output/                  # Results will appear here
├── logs/                        # Log files
├── tests/                       # Test suite
├── main.py                      # Main entry point
├── setup.py                     # Installation script
├── test_system.py               # System test
├── config.yaml                  # Configuration file
├── requirements.txt             # Dependencies
├── .env.example                 # Environment template
├── README.md                    # Main documentation
├── QUICKSTART.md                # Quick start guide
├── RELEASE_NOTES.md             # Release information
├── SYSTEM_STATUS.md             # System status
├── REAL_DIALOGS_RESULTS.md      # Test results
└── DEPLOYMENT.md                # This file
```

## 🔧 Installation

### 1. Clone Repository
```bash
git clone <repository-url>
cd dialogs-rag-v2
```

### 2. Install Dependencies
```bash
python setup.py
```

### 3. Configure Environment
```bash
# Copy environment template
cp .env.example .env

# Edit .env file and add your OpenAI API key
echo "OPENAI_API_KEY=your_api_key_here" > .env
```

### 4. Test Installation
```bash
python test_system.py
```

## 🚀 Usage

### Basic Usage
```bash
# Process your dialogs
python main.py --input data/input/dialogs.xlsx --verbose
```

### Advanced Usage
```bash
# Custom configuration
python main.py --input data/input/dialogs.xlsx --config custom_config.yaml --verbose

# Custom output directory
python main.py --input data/input/dialogs.xlsx --output results/ --verbose

# Dry run (test without processing)
python main.py --input data/input/dialogs.xlsx --dry-run --verbose
```

## 📊 Monitoring

### Check System Status
```bash
python test_system.py
```

### View Logs
```bash
tail -f logs/pipeline.log
```

### Check Results
```bash
ls -la data/output/
```

## 🔧 Configuration

### Main Configuration (config.yaml)
```yaml
openai:
  model: "gpt-4o-mini"        # Model to use
  temperature: 0.1            # Response randomness
  max_tokens: 4000           # Max tokens per request

processing:
  batch_size: 20             # Dialogs per batch
  max_workers: 4             # Parallel workers
  retry_attempts: 3          # Retry failed requests
```

### Environment Variables (.env)
```bash
OPENAI_API_KEY=your_api_key_here
LOG_LEVEL=INFO
DATA_DIR=./data
OUTPUT_DIR=./data/output
```

## 📈 Performance Tuning

### For Large Datasets
```yaml
# config.yaml
processing:
  batch_size: 10             # Smaller batches
  retry_delay: 2             # Longer delays
```

### For Speed
```yaml
# config.yaml
processing:
  batch_size: 50             # Larger batches
  max_workers: 8             # More workers
```

## 🐛 Troubleshooting

### Common Issues

1. **API Key Error**
   ```bash
   # Check .env file
   cat .env
   # Should contain: OPENAI_API_KEY=your_key_here
   ```

2. **Rate Limit Error**
   ```bash
   # Reduce batch size in config.yaml
   batch_size: 5
   ```

3. **Memory Error**
   ```bash
   # Reduce batch size and workers
   batch_size: 5
   max_workers: 2
   ```

### Debug Mode
```bash
# Enable debug logging
export LOG_LEVEL=DEBUG
python main.py --input data/input/dialogs.xlsx --verbose
```

## 📁 Data Formats

### Supported Input Formats
- **Excel** (.xlsx) - Recommended
- **CSV** (.csv)
- **JSON** (.json)

### Required Columns
- `text` - Dialog text content
- `dialog_id` - Unique dialog identifier

### Example Input
```csv
dialog_id,text
1,"Оператор: Здравствуйте. Клиент: Здравствуйте..."
2,"Оператор: Добрый день. Клиент: Добрый день..."
```

## 📊 Output Files

### Generated Files
- `dialog_processing_*.pkl` - Processed dialogs
- `entity_extraction_*.pkl` - Extracted entities
- `pipeline_results_*.json` - Final results
- `cluster_analysis_*.json` - Cluster analysis (if successful)

### Results Structure
```json
{
  "final_results": {
    "total_dialogs": 20,
    "total_entities": 100,
    "success_rate": 1.0
  },
  "stages": {
    "dialog_processing": {"status": "completed"},
    "entity_extraction": {"status": "completed"}
  }
}
```

## 🔒 Security

### API Key Protection
- Never commit `.env` file to version control
- Use environment variables in production
- Rotate API keys regularly

### Data Privacy
- Process sensitive data locally
- Don't log sensitive information
- Use secure file permissions

## 📞 Support

### Getting Help
1. Check logs in `logs/` directory
2. Run `python test_system.py`
3. Review documentation
4. Check GitHub issues

### Reporting Issues
- Include log files
- Describe input data format
- Specify error messages
- Provide system information

---

**Ready for production deployment!** 🚀
