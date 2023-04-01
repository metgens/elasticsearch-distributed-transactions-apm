FROM mcr.microsoft.com/dotnet/aspnet:6.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:6.0 AS build
WORKDIR /src
COPY ["apm-example.sender/apm-example.sender.csproj", "apm-example.sender/"]
RUN dotnet restore "apm-example.sender/apm-example.sender.csproj"
COPY . .
WORKDIR "/src/apm-example.sender"
RUN dotnet build "apm-example.sender.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "apm-example.sender.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "apm-example.sender.dll"]
