# Airflow Plugins

This directory contains custom Airflow plugins, operators, hooks, and sensors.

## Structure

- `operators/` - Custom operators for specific tasks
- `hooks/` - Custom hooks for external system connections
- `sensors/` - Custom sensors for monitoring and triggering

## Usage

Plugins placed here are automatically discovered by Airflow and can be imported in DAGs.
