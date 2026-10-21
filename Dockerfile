FROM node:22-bookworm AS node

FROM hexpm/elixir:1.17.3-erlang-27.3.4.13-debian-bookworm-20260623-slim AS build

ENV MIX_ENV=prod LANG=C.UTF-8
COPY --from=node /usr/local/bin/node /usr/local/bin/node
COPY --from=node /usr/local/lib/node_modules /usr/local/lib/node_modules
RUN ln -s /usr/local/lib/node_modules/npm/bin/npm-cli.js /usr/local/bin/npm
RUN apt-get update && apt-get install -y --no-install-recommends build-essential git ca-certificates \
    && rm -rf /var/lib/apt/lists/*
RUN mix local.hex --force && mix local.rebar --force

WORKDIR /src
COPY mix.exs mix.lock ./
COPY config ./config
RUN mix deps.get --only prod && mix deps.compile
COPY assets/package.json assets/package-lock.json ./assets/
RUN npm ci --prefix assets
COPY . .
RUN npm run deploy --prefix assets && mix phx.digest && mix compile && mix release

FROM debian:bookworm-slim AS runtime

# Outgoing TLS trusts this store. Refuse images with the compromised emSign root.
RUN apt-get update && apt-get install -y --no-install-recommends git python3 ca-certificates libssl3 libstdc++6 libncurses6 zlib1g \
    && rm -rf /var/lib/apt/lists/* \
    && test ! -e /etc/ssl/certs/emSign_ECC_Root_CA_-_G3.pem
WORKDIR /app
COPY --from=build /src/_build/prod/rel/bors ./
COPY deploy/cloudflare/entrypoint.sh /usr/local/bin/bors-entrypoint
RUN chmod 755 /usr/local/bin/bors-entrypoint
ENV PORT=4000 DATABASE_AUTO_MIGRATE=true LANG=C.UTF-8
EXPOSE 4000
ENTRYPOINT ["/usr/local/bin/bors-entrypoint"]
