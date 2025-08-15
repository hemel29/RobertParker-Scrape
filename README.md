# Robert Parker Wine Scraper GUI

A powerful GUI application for scraping wine data from Robert Parker's website using Python, Playwright, and Tkinter.

## Features

- **Graphical User Interface**: Easy-to-use Tkinter-based GUI
- **Concurrent Scraping**: Scrape multiple URLs simultaneously (configurable)
- **Rate Limiting**: Built-in rate limiting to avoid being blocked
- **Excel Export**: Automatically saves results to Excel files
- **Error Handling**: Comprehensive error handling and logging
- **Progress Tracking**: Real-time progress updates and speed monitoring

## Prerequisites

- Python 3.7 or higher
- Windows, macOS, or Linux operating system
- Internet connection
- Robert Parker website account (email and password)

## Installation

### 1. Install Python Dependencies

```bash
pip install playwright openpyxl
```

### 2. Install Playwright Browsers

```bash
playwright install
```

### 3. Verify Installation

```bash
python -c "import playwright, openpyxl, tkinter; print('All dependencies installed successfully!')"
```

## Usage

### 1. Launch the Application

```bash
python robert_parker_gui_scraper.py
```

### 2. Configure Settings

- **Email & Password**: Enter your Robert Parker website credentials
- **Max Concurrent Requests**: Number of URLs to scrape simultaneously (1-20, default: 5)
- **Requests per Minute**: Rate limiting (10-100, default: 30)

### 3. Add Wine URLs

Enter wine URLs one per line in the text area:

```
https://www.robertparker.com/wines/example-wine-url-1
https://www.robertparker.com/wines/example-wine-url-2
```

### 4. Start Scraping

Click **"Start Scraping"** to begin the process.

### 5. Monitor Progress

- Progress bar shows completion percentage
- Real-time speed and time tracking
- URL progress counter
- Detailed log output

### 6. View Results

Results are automatically saved to an Excel file with timestamp.

## Data Fields Extracted

- Full_Wine_Name, Wine_Name, Vintage
- Producer, Wine Region, Variety, Color
- Score, Drink Window, Reviewed By
- Release Price, Drink Date
- Tasting Note, Producer Note
- Maturity, Certified, Published Date

## Performance Settings

- **Small batches (1-10 URLs)**: 3-5 concurrent, 30 req/min
- **Medium batches (10-50 URLs)**: 5-8 concurrent, 25 req/min
- **Large batches (50+ URLs)**: 8-10 concurrent, 20 req/min

## Error Handling

- Export error logs to CSV
- Comprehensive error tracking
- Automatic retry mechanisms
- Detailed failure reporting

## Troubleshooting

### Common Issues

1. **Module not found**: Install dependencies with `pip install playwright openpyxl`
2. **Login failures**: Verify credentials and reduce concurrent requests
3. **Browser crashes**: Delete `browser_data` folder and restart
4. **Slow performance**: Reduce concurrent requests and rate limits

### Debug Mode

Browser runs in non-headless mode by default for troubleshooting. To enable headless mode, modify line 47:

```python
headless=True,  # Change from False to True
```

## File Structure

```
robert-parker-scraper/
├── robert_parker_gui_scraper.py    # Main application
├── README.md                        # This documentation
├── browser_data/                    # Browser session data
├── robert_parker_cookies.json      # Saved cookies
└── robert_parker_wines_*.xlsx      # Output files
```

## Legal Considerations

- Comply with Robert Parker's terms of service
- Use reasonable scraping rates
- Respect copyright and data usage policies
- Intended for personal research and analysis

## Support

For issues:
1. Check troubleshooting section
2. Review error logs in application
3. Verify credentials and internet connection
4. Reduce performance settings
5. Check website structure changes

---

**Note**: Use responsibly and in accordance with website terms of service.

