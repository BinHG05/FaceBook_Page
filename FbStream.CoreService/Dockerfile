FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
WORKDIR /src

COPY ["FbStream.CoreService/FbStream.CoreService.csproj", "FbStream.CoreService/"]
COPY ["FbStream.Contracts/FbStream.Contracts.csproj", "FbStream.Contracts/"]
RUN dotnet restore "FbStream.CoreService/FbStream.CoreService.csproj"

COPY . .
RUN dotnet publish "FbStream.CoreService/FbStream.CoreService.csproj" -c Release -o /app/publish /p:UseAppHost=false

FROM mcr.microsoft.com/dotnet/aspnet:9.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
ENV ASPNETCORE_URLS=http://+:8080
EXPOSE 8080
ENTRYPOINT ["dotnet", "FbStream.CoreService.dll"]
