const buildResponse = (code, body) =>{
    return {
        statusCode: code,
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(body)
    }
};

module.exports = {
    buildResponse
};