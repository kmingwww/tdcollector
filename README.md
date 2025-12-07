# Data Collector
1. Sign in to the platform
2. Ctrl + Shift + C to open developer console
3. Go to network tab, copy `Cookie` value in any request 
4. Replace the `COOKIE` value you copied in step 3 in `tm/api.py` file
5. Run `uv run main.py ongoing` for ongoing order
6. Run `uv run main.py historical 2025 04` for historical order, 2025 is the year and 04 is the month

# Guidelines
- https://www.dash0.com/guides/logging-in-python
- https://www.youtube.com/watch?v=9L77QExPmI0
