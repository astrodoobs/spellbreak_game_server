FROM brendoncintas/spellbreak_game_server:latest

RUN pip3 install --no-cache-dir aiosqlite

ENV WINEDLLOVERRIDES=steam_api64=n,b;match_tracker.dll=n,b

COPY elefrac/            /spellbreak-server/elefrac/
COPY elefrac.config.ini  /spellbreak-server/config.ini
COPY docker-entrypoint.sh /spellbreak-server/docker-entrypoint.sh

# Mod DLLs — overlay on top of base image
COPY BaseServer/Mods/ /spellbreak-server/BaseServer/Mods/
COPY BaseServer/g3/Binaries/Win64/ /spellbreak-server/BaseServer/g3/Binaries/Win64/
COPY BaseServer/Engine/Binaries/ThirdParty/Steamworks/Steamv150/Win64/steam_api64.dll \
     /spellbreak-server/BaseServer/Engine/Binaries/ThirdParty/Steamworks/Steamv150/Win64/steam_api64.dll
RUN chmod +x /spellbreak-server/docker-entrypoint.sh

WORKDIR /spellbreak-server
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["/spellbreak-server/docker-entrypoint.sh"]
