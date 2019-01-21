﻿module JobsNearby.Api.App

open System
open System.IO
open Microsoft.AspNetCore.Builder
open Microsoft.AspNetCore.Cors.Infrastructure
open Microsoft.AspNetCore.Hosting
open Microsoft.Extensions.Logging
open Microsoft.Extensions.DependencyInjection
open Giraffe
open Giraffe.HttpStatusCodeHandlers.Successful
open Giraffe.HttpStatusCodeHandlers.RequestErrors
open Giraffe.ModelBinding
open JobsNearby.Api.HttpHandlers
open Microsoft.Extensions.Configuration

// ---------------------------------
// Web app
// ---------------------------------

let webApp =
    choose [
        GET >=> route "/" >=> simpleAuthn >=> htmlFile "Public/index.html"
        subRoute "/api"
            (simpleAuthn >=> choose [
                GET >=> choose [
                    route "/profiles" >=> getAllProfiles
                    routef "/profiles/%i/datasets" getAllDataSets
                    route "/jobdata" >=> tryBindQuery BAD_REQUEST None getJobData
                    routef "/test/%s" test
                ]
                POST >=> choose [
                    routef "/crawling/%s" crawling
                    route "/company/geo" >=> tryBindForm BAD_REQUEST None searchAndUpdateCompGeo
                    routef "/compay/doubt/p/%s" companyDoubt                    
                ]
            ])
        POST >=> route "/worker/start" >=> workOnBacklog
        setStatusCode 404 >=> text "Not Found" ]

// ---------------------------------
// Error handler
// ---------------------------------

let errorHandler (ex : Exception) (logger : ILogger) =
    logger.LogError(ex, "An unhandled exception has occurred while executing the request.")
    clearResponse >=> setStatusCode 500 >=> text ex.Message

// ---------------------------------
// Config and Main
// ---------------------------------

let configureAppConfig (ctx: WebHostBuilderContext) (config: IConfigurationBuilder) =
    let environment = ctx.HostingEnvironment.EnvironmentName
    config
        .SetBasePath(System.IO.Directory.GetCurrentDirectory())
        .AddJsonFile("appsettings.json", optional = true)
        .AddJsonFile(sprintf "appsettings.%s.json" environment, optional = true)
        .AddEnvironmentVariables()
    |> ignore


let configureCors (builder : CorsPolicyBuilder) =
    builder.WithOrigins("http://localhost:8080")
           .AllowAnyMethod()
           .AllowAnyHeader()
           |> ignore

let configureApp (app : IApplicationBuilder) =
    let env = app.ApplicationServices.GetService<IHostingEnvironment>()
    (match env.IsDevelopment() with
    | true  -> app.UseDeveloperExceptionPage()
    | false -> app.UseGiraffeErrorHandler errorHandler)
        .UseHttpsRedirection()
        .UseCors(configureCors)
        .UseStaticFiles()
        .UseGiraffe(webApp)

let configureServices (services : IServiceCollection) =
    services.AddCors()    |> ignore
    services.AddGiraffe() |> ignore

let configureLogging (builder : ILoggingBuilder) =
    builder.AddFilter(fun l -> l.Equals LogLevel.Error)
           .AddConsole()
           .AddDebug() |> ignore

let contentRoot = Directory.GetCurrentDirectory()
let webRoot = Path.Combine(contentRoot, "Public")

[<EntryPoint>]
let main _ =
    WebHostBuilder()
        .UseKestrel()
        .UseContentRoot(contentRoot)
        .UseIISIntegration()
        .UseWebRoot(webRoot)
        .ConfigureAppConfiguration(configureAppConfig)
        .Configure(Action<IApplicationBuilder> configureApp)
        .ConfigureServices(configureServices)
        .ConfigureLogging(configureLogging)
        .Build()
        .Run()
    0