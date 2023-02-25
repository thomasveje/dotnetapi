FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /app

COPY dotnetapi.csproj ./dotnetapi.csproj
COPY ./src ./src

RUN dotnet publish "dotnetapi.csproj" -c Release -o /app/publish

# FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS runtime
FROM mcr.microsoft.com/dotnet/sdk:6.0 AS runtime
RUN apt-get update -yq && apt-get install -yq curl git nano
WORKDIR /app
COPY --from=build /app/publish .
RUN chown -R 1001:0 /app && \
    chmod -R g=u /app && \
    chown -R 1001:0 /root/.nuget && \
    chmod -R g=u /root/.nuget && \
    mkdir -p /tmp/home/.local/share/NuGet/Migrations && \
    chown -R 1001:0 /tmp/home && \
    chmod -R g=u /tmp/home
USER 1001
ENV DOTNET_BUNDLE_EXTRACT_BASE_DIR=/tmp
ENV DOTNET_BUNDLE_EXTRACT_DIR=/tmp
ENV DOTNET_CLI_HOME=/tmp/dotnetclihome
ENV DOTNET_USER_HOME=/tmp/home
ENV HOME=/tmp/home

ENTRYPOINT ["dotnet", "dotnetapi.dll"]

