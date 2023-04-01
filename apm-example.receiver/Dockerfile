FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["apm-example.receiver/apm-example.receiver.csproj", "apm-example.receiver/"]
RUN dotnet restore "apm-example.receiver/apm-example.receiver.csproj"
COPY . .
WORKDIR "/src/apm-example.receiver"
RUN dotnet build "apm-example.receiver.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "apm-example.receiver.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "apm-example.receiver.dll"]
