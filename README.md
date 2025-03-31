# README.md

# Azure File Share Report

This is a simple console application built in C# to export a list of files properties in a Azure file share.

## Prerequisites
To work with this application, ensure you have the following installed on your machine:

- .NET SDK (version 8.0 or later)
- A code editor like Visual Studio Code or Visual Studio (optional)

## Getting Started

Follow these steps to set up and run the application locally.

### Setup Locally

1. Clone the repository or download the project files to your local machine.
2. Open a terminal and navigate to the project directory.

### Restore Dependencies

Run the following command to restore the required dependencies:

```
dotnet restore
```

### Build the Application

To build the application, use the following command:

```
dotnet build
```

### Run the Application

After building the application, you can run it using:

```
dotnet run
```
The console will ask for the storage account connection string, paste it and press enter. Repeat when asked for share name.

The results will be printed in the console and in a csv file in the running directory.

## Project Structure

```
fileShareReport
├── src
│   └── Program.cs
├── fileShareReport.csproj
└── README.md
```

## License

This project is licensed under the MIT License.