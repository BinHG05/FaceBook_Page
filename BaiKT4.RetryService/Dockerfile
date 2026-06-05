FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["BaiKT4.RetryService/BaiKT4.RetryService.csproj", "BaiKT4.RetryService/"]
COPY ["BaiKT4.Contracts/BaiKT4.Contracts.csproj", "BaiKT4.Contracts/"]
RUN dotnet restore "BaiKT4.RetryService/BaiKT4.RetryService.csproj"

COPY . .
RUN dotnet publish "BaiKT4.RetryService/BaiKT4.RetryService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "BaiKT4.RetryService.dll"]
