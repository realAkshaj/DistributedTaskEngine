# syntax=docker/dockerfile:1
FROM mcr.microsoft.com/dotnet/sdk:8.0 AS builder

WORKDIR /src
COPY src/Contracts    src/Contracts
COPY src/Orchestrator src/Orchestrator

WORKDIR /src/src/Orchestrator
RUN dotnet publish -c Release -o /app --no-self-contained

FROM mcr.microsoft.com/dotnet/aspnet:8.0 AS runtime

WORKDIR /app
COPY --from=builder /app ./

ENV ASPNETCORE_URLS=""
EXPOSE 8080 5001

ENTRYPOINT ["dotnet", "Dte.Orchestrator.dll"]
