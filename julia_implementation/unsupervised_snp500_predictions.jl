using CSV
using HTTP
using ZipFile
using DataFrames
using HTMLTables

function read_ff_data()
    url = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_CSV.zip"
    # set header
    headers = ["User-Agent" => "Mozilla/5.0"]

    # fetch data
    response = HTTP.get(url, headers)
    bytes = response.body # Vector{UInt8} object

    # unzip
    z = ZipFile.Reader(IOBuffer(bytes))
    csv_file = z.files[1]

    # read csv and load as a DataFrame object

    data_text = String(read(csv_file))
    monthly_data = split(data_text, "\r\n\r\n")[2]
    ff_factors = CSV.read(IOBuffer(monthly_data), DataFrame)
    
    # close file
    close(z)

    return ff_factors
end

function read_sp_data()
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # set header
    headers = ["User-Agent" => "Mozilla/5.0"]

    # fetch data
    response = HTTP.get(url, headers)
    html = String(response.body) # Vector{UInt8} object
    
    # read the html table and load as a DataFrame object
    tables = readtable(html)
    sp500 = DataFrame(tables[1])

    return sp500
end

function main()
    ff_factors = read_ff_data()
    sp500 = read_sp_data()

    println(first(ff_factors, 5))
    println(first(sp500, 5))

end

main()
