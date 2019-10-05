module JobsNearby.Api.App

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
open Serilog
open Giraffe.SerilogExtensions
open JobsNearby.Api.Models
open JobsNearby.Api.Services
open Serilog.Formatting.Json

// ---------------------------------
// Web app
// ---------------------------------

Serilog.Debugging.SelfLog.Enable(Console.Error)

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

let serilogConfig =
    { SerilogConfig.defaults with
        IgnoredRequestFields = 
            Ignore.fromRequest
            |> fun (Choser lst) -> Choser ("Response.ContentLength" :: lst)
        ErrorHandler = 
            fun ex httpContext ->
                clearResponse >=> setStatusCode 500 >=> text ex.Message}

let webAppWithLogging = SerilogAdapter.Enable(webApp, serilogConfig)

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
    | false -> app)
        .UseHttpsRedirection()
        .UseCors(configureCors)
        .UseStaticFiles()
        // .UseAuthentication()
        .UseGiraffe(webAppWithLogging)

let configureServices (hostCtx: WebHostBuilderContext) (services : IServiceCollection) =
    let config = hostCtx.Configuration
    services.AddCors()    |> ignore
    services.AddGiraffe() |> ignore
    services.Configure<AppSetting>(config) |> ignore
    services.AddScoped<IGeoService>(GeoServiceProvider) |> ignore
    services.AddScoped<ICompStore>(CompStoreProvider) |> ignore
    services.AddScoped<IProfileStore>(ProfileStoreProvider) |> ignore
    services.AddScoped<IJobDataStore>(JobDataStoreProvider) |> ignore
    services.AddScoped<IBacklogQueue>(BacklogQueueProvider) |> ignore

let serilogInit (hostCtx: WebHostBuilderContext) (loggerConfig: LoggerConfiguration) = 
    loggerConfig
        .ReadFrom.Configuration(hostCtx.Configuration)
        .Enrich.FromLogContext()
        .Destructure.FSharpTypes()
        // .WriteTo.Console()
        // .WriteTo.Console(new JsonFormatter())
    |> ignore

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
        .UseSerilog(serilogInit)
        .ConfigureServices(configureServices)
        .Build()
        .Run()
    0