# PII Analysis Dashboard

A Flask web application that provides real-time visualization of PII analysis progress and results.

## Features

- Progress monitoring for PII analysis jobs
- File type statistics and charts
- Entity type statistics and charts
- High-risk file identification
- Error analysis and categorization
- Mobile-responsive design
- Auto-refreshing data

## Screenshots

(Add screenshots once the dashboard is running)

## Requirements

- Python 3.6+
- Flask and dependencies (see requirements.txt)
- Access to a PII analysis database

## Installation

1. Install the required packages:

```bash
pip install -r requirements.txt
```

2. Set up environment variables (optional):

```bash
export PII_DB_PATH=/path/to/your/pii_results.db
export PORT=5000  # Change if needed
```

## Usage

### Running the Dashboard

Run the dashboard with the default database path:

```bash
python app.py
```

Or specify a custom database path:

```bash
python app.py --db-path=/path/to/your/pii_results.db
```

### Accessing the Dashboard

Once running, access the dashboard at:

```
http://localhost:5000
```

### Cloudflare Tunnel (Optional)

To access the dashboard from outside your network, you can set up a Cloudflare tunnel:

1. Install cloudflared:

```bash
# On macOS
brew install cloudflare/cloudflare/cloudflared

# On Linux
# Follow instructions at https://developers.cloudflare.com/cloudflare-one/connections/connect-apps/install-and-setup/installation
```

2. Run a tunnel to your dashboard:

```bash
cloudflared tunnel --url http://localhost:5000
```

3. Access the dashboard using the provided URL.

## Development

The dashboard is built with:

- Flask backend
- Bootstrap 5 CSS framework
- Chart.js for data visualization
- JavaScript for interactivity

### Project Structure

```
dashboard/
├── app.py                 # Flask application
├── templates/             # HTML templates
│   └── index.html         # Main dashboard template
├── static/                # Static files
│   ├── css/               # CSS stylesheets
│   │   └── dashboard.css  # Dashboard styles
│   ├── js/                # JavaScript files
│   │   └── dashboard.js   # Dashboard functionality
│   └── img/               # Images and icons
│       └── logo.svg       # Dashboard logo
└── README.md              # This file
```

## License

This project is part of the PII analysis system.

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request. 