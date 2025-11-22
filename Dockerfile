FROM apify/actor-node:20

COPY package*.json ./

# Install all dependencies (including TypeScript compiler)
RUN npm --quiet set progress=false \
 && npm install

# Copy the source code
COPY . ./

# THE MAGIC STEP: Turn TypeScript into JavaScript
RUN npm run build

# The command to run when the actor starts (points to the built file)
CMD [ "npm", "start" ]