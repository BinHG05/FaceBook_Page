FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["FbStream.WebhookService/FbStream.WebhookService.csproj", "FbStream.WebhookService/"]
COPY ["FbStream.Contracts/FbStream.Contracts.csproj", "FbStream.Contracts/"]
RUN dotnet restore "FbStream.WebhookService/FbStream.WebhookService.csproj"

COPY . .
RUN dotnet publish "FbStream.WebhookService/FbStream.WebhookService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "FbStream.WebhookService.dll"]
