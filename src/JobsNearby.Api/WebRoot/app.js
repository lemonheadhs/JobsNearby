function triggerCrawling() {
    axios.post("api/jobdata/crawl")
        .then(resp => console.log(resp))
}