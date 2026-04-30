# Use Node 20 as base
FROM node:20-slim

# Create app directory
WORKDIR /app

# Install build dependencies for better-sqlite3 (Python, G++, Make)
RUN apt-get update && apt-get install -y \
    python3 \
    make \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy package files
COPY package*.json ./

# Install dependencies
RUN npm install

# Copy source code
COPY . .

# Build TypeScript (optional, but good for production)
# We'll use ts-node for now as requested for simplicity, 
# but a build step is better for high-scale.
RUN npx tsc

# The actual command is handled by docker-compose
