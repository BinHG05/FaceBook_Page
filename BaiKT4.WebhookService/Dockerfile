FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["BaiKT4.WebhookService.csproj", "./"]
RUN dotnet restore "BaiKT4.WebhookService.csproj"

COPY . .
RUN dotnet publish "BaiKT4.WebhookService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "BaiKT4.WebhookService.dll"]
